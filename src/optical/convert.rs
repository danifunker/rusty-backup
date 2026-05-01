use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::backup::{LogLevel, LogMessage};

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
    chd_options: Option<crate::rbformats::chd_options::ChdOptions>,
    progress: Arc<Mutex<ConvertProgress>>,
) -> Result<()> {
    set_operation(&progress, "Converting to CHD...");

    let format = opticaldiscs::DiscFormat::from_path(input_path)
        .with_context(|| format!("Unrecognized format: {}", input_path.display()))?;

    let opts = match chd_options {
        Some(co) => libchdman_rs::cd::CdCreateOptions {
            hunk_size: co.hunk_size,
            codecs: co.codecs,
        },
        None => libchdman_rs::cd::CdCreateOptions::default(),
    };
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

/// Extract a CD CHD into a BIN/CUE pair via libchdman-rs.
///
/// libchdman-rs's `cd::extract_to_cue` writes the BIN next to the CUE
/// (filename derived from the CUE) and emits chdman-equivalent CUE
/// metadata (per-track MODE/AUDIO + INDEX/PREGAP/POSTGAP). Tracks with
/// stored subcode are silently dropped (CUE/BIN can't carry it); we log
/// a one-liner so the user isn't surprised.
pub fn chd_to_bincue(
    chd_path: &Path,
    cue_path: &Path,
    progress: Arc<Mutex<ConvertProgress>>,
) -> Result<()> {
    set_operation(&progress, "Extracting CHD to BIN/CUE...");

    let bin_path = cue_path.with_extension("bin");

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Extracting CHD: {} -> {} + {}",
            chd_path.display(),
            cue_path.display(),
            bin_path.display()
        ),
    );

    // Pre-flight: warn if any track stored subcode that the CUE/BIN
    // pair will lose.
    if let Ok(chd) = libchdman_rs::Chd::open(
        chd_path
            .to_str()
            .with_context(|| format!("CHD path is not valid UTF-8: {}", chd_path.display()))?,
        false,
        None,
    ) {
        if let Ok(tracks) = libchdman_rs::cd::list_tracks(&chd) {
            if tracks
                .iter()
                .any(|t| t.subcode_type != libchdman_rs::cd::SubcodeType::None)
            {
                log(
                    &progress,
                    LogLevel::Info,
                    "Subcode data dropped (CUE/BIN format limitation)",
                );
            }
        }
    }

    let progress_for_cb = Arc::clone(&progress);
    let mut on_progress = move |bytes_done: u64| {
        if let Ok(mut s) = progress_for_cb.lock() {
            s.current_bytes = bytes_done;
        }
    };

    libchdman_rs::cd::extract_to_cue(chd_path, cue_path, &bin_path, &mut on_progress)
        .map_err(|e| anyhow::anyhow!("CD CHD extract to BIN/CUE failed: {:?}", e))?;

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

/// Extract a CD CHD into a multi-bin BIN/CUE pair (one .bin per track).
///
/// libchdman-rs (and chdman) only emit single-bin output. Multi-bin layout is
/// a feature beyond chdman: each track gets its own `<base> (Track NN).bin`
/// and the cue references them via separate `FILE` lines, with each file's
/// MSF restarting at `00:00:00`.
///
/// Implementation: extract single-bin first via [`chd_to_bincue`], then split
/// the resulting `.bin` along track boundaries and rewrite the cue. Track byte
/// boundaries come from `list_tracks` (frames per track) and the per-track
/// `MODE1/2352` etc. token in the cue (datasize per track). When
/// `sum(frames * datasize) != bin_size` (typically only happens when the CHD
/// has stored padframes/splitframes the public API can't see), we fall back to
/// single-bin output and log a warning so the user isn't surprised.
pub fn chd_to_bincue_multi(
    chd_path: &Path,
    cue_path: &Path,
    progress: Arc<Mutex<ConvertProgress>>,
) -> Result<()> {
    // Step 1: produce single-bin via libchdman-rs.
    chd_to_bincue(chd_path, cue_path, Arc::clone(&progress))?;

    let single_bin = cue_path.with_extension("bin");

    // Step 2: read the cue we just wrote and the track list. Both come from
    // libchdman-rs so we have a tight contract on the format.
    let cue_text = std::fs::read_to_string(cue_path)
        .with_context(|| format!("failed to read cue {}", cue_path.display()))?;
    let parsed = parse_libchdman_cue(&cue_text).context("failed to parse generated cue")?;

    let chd = libchdman_rs::Chd::open(
        chd_path
            .to_str()
            .with_context(|| format!("CHD path is not valid UTF-8: {}", chd_path.display()))?,
        false,
        None,
    )
    .map_err(|e| anyhow::anyhow!("open CHD failed: {:?}", e))?;
    let tracks = libchdman_rs::cd::list_tracks(&chd)
        .map_err(|e| anyhow::anyhow!("list CHD tracks failed: {:?}", e))?;

    if tracks.len() != parsed.tracks.len() {
        bail!(
            "track count mismatch between CHD ({}) and cue ({}) — multi-bin split aborted",
            tracks.len(),
            parsed.tracks.len(),
        );
    }

    let bin_size = std::fs::metadata(&single_bin)
        .with_context(|| format!("failed to stat {}", single_bin.display()))?
        .len();
    let claimed: u64 = tracks
        .iter()
        .zip(parsed.tracks.iter())
        .map(|(t, p)| u64::from(t.frames) * u64::from(p.datasize))
        .sum();

    if claimed != bin_size {
        log(
            &progress,
            LogLevel::Warning,
            format!(
                "multi-bin: track sizes (sum {}) don't match bin size ({}); the CHD likely has \
                 padframes/splitframes that aren't exposed publicly — falling back to single-bin output",
                claimed, bin_size,
            ),
        );
        // Single-bin output is already on disk from step 1; nothing else to do.
        if let Ok(mut p) = progress.lock() {
            p.finished = true;
        }
        return Ok(());
    }

    // Step 3: split the bin into per-track files. Stream rather than slurp so
    // we don't peak at the full bin size in RAM (CDs commonly run > 700 MB).
    let stem = cue_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid cue path: {}", cue_path.display()))?
        .to_string();
    let parent = cue_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| Path::new(".").to_path_buf());

    let mut bin_reader = BufReader::with_capacity(
        64 * 1024,
        File::open(&single_bin).with_context(|| format!("open {}", single_bin.display()))?,
    );

    let mut per_track_bins: Vec<std::path::PathBuf> = Vec::with_capacity(tracks.len());
    let mut buf = vec![0u8; 64 * 1024];
    for (i, t) in tracks.iter().enumerate() {
        let track_bytes = u64::from(t.frames) * u64::from(parsed.tracks[i].datasize);
        let track_bin = parent.join(format!("{} (Track {:02}).bin", stem, i + 1));
        let mut writer = BufWriter::with_capacity(
            64 * 1024,
            File::create(&track_bin).with_context(|| format!("create {}", track_bin.display()))?,
        );
        let mut remaining = track_bytes;
        while remaining > 0 {
            let to_read = (remaining as usize).min(buf.len());
            let n = bin_reader
                .read(&mut buf[..to_read])
                .with_context(|| format!("read {}", single_bin.display()))?;
            if n == 0 {
                bail!(
                    "unexpected EOF in {} while splitting track {}",
                    single_bin.display(),
                    i + 1
                );
            }
            writer
                .write_all(&buf[..n])
                .with_context(|| format!("write {}", track_bin.display()))?;
            remaining -= n as u64;
        }
        writer.flush().ok();
        per_track_bins.push(track_bin);
    }

    drop(bin_reader);

    // Step 4: rewrite the cue with per-FILE entries and fresh MSF offsets.
    let mut cue_writer = BufWriter::new(
        File::create(cue_path).with_context(|| format!("create {}", cue_path.display()))?,
    );
    for (i, parsed_track) in parsed.tracks.iter().enumerate() {
        let bin_name = per_track_bins[i]
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("track.bin");
        writeln!(cue_writer, "FILE \"{}\" BINARY", bin_name)?;
        writeln!(
            cue_writer,
            "  TRACK {:02} {}",
            i + 1,
            parsed_track.mode_token
        )?;
        if let Some(pregap) = &parsed_track.pregap_msf {
            writeln!(cue_writer, "    PREGAP {}", pregap)?;
        }
        if parsed_track.has_index_00 {
            writeln!(cue_writer, "    INDEX 00 00:00:00")?;
            // INDEX 01 sits `pregap_data_frames` after INDEX 00. The original
            // cue has INDEX 01 at frame_offset + pregap; we only need the
            // delta, which equals tracks[i].pregap when pgdatasize > 0.
            let pregap_frames = tracks[i].pregap;
            writeln!(cue_writer, "    INDEX 01 {}", msf_string(pregap_frames))?;
        } else {
            writeln!(cue_writer, "    INDEX 01 00:00:00")?;
        }
        if let Some(postgap) = &parsed_track.postgap_msf {
            writeln!(cue_writer, "    POSTGAP {}", postgap)?;
        }
    }
    cue_writer.flush()?;

    // Step 5: remove the single-bin (it's been split).
    std::fs::remove_file(&single_bin)
        .with_context(|| format!("remove single-bin {}", single_bin.display()))?;

    log(
        &progress,
        LogLevel::Info,
        format!(
            "multi-bin extraction complete: {} ({} tracks)",
            cue_path.display(),
            tracks.len()
        ),
    );
    if let Ok(mut p) = progress.lock() {
        p.finished = true;
    }
    Ok(())
}

/// Format a frame count as `MM:SS:FF` (75 frames/second).
fn msf_string(frames: u32) -> String {
    let m = frames / (75 * 60);
    let s = (frames / 75) % 60;
    let f = frames % 75;
    format!("{:02}:{:02}:{:02}", m, s, f)
}

#[derive(Debug, Clone)]
struct ParsedTrack {
    /// Cue mode token, e.g. `MODE1/2352` or `AUDIO`.
    mode_token: String,
    /// Bytes per sector for this track — derived from `mode_token`. AUDIO is 2352.
    datasize: u32,
    /// `Some(msf)` when the cue carries a `PREGAP MM:SS:FF` line (pgdatasize == 0).
    pregap_msf: Option<String>,
    /// True when the cue carries an `INDEX 00` line (pgdatasize > 0 → pregap data is in bin).
    has_index_00: bool,
    /// `Some(msf)` when the cue carries a `POSTGAP MM:SS:FF` line.
    postgap_msf: Option<String>,
}

#[derive(Debug, Default)]
struct ParsedCue {
    tracks: Vec<ParsedTrack>,
}

/// Tight parser for libchdman-rs's cue output (not a general cue parser).
/// Recognizes `FILE … BINARY`, `TRACK NN <mode>`, `INDEX 00/01 MSF`,
/// `PREGAP MSF`, `POSTGAP MSF`. Unknown lines are tolerated and skipped.
fn parse_libchdman_cue(text: &str) -> Result<ParsedCue> {
    let mut out = ParsedCue::default();
    let mut current: Option<ParsedTrack> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("TRACK ") {
            if let Some(prev) = current.take() {
                out.tracks.push(prev);
            }
            // "TRACK NN <mode_token>"
            let mut parts = rest.splitn(2, ' ');
            let _num = parts.next();
            let mode = parts.next().unwrap_or("").trim().to_string();
            let datasize = if mode.starts_with("MODE") {
                // "MODE1/2352" or "MODE2/2336"
                mode.split('/')
                    .nth(1)
                    .and_then(|n| n.parse::<u32>().ok())
                    .unwrap_or(2352)
            } else {
                // AUDIO is always 2352
                2352
            };
            current = Some(ParsedTrack {
                mode_token: mode,
                datasize,
                pregap_msf: None,
                has_index_00: false,
                postgap_msf: None,
            });
            continue;
        }
        if let Some(t) = current.as_mut() {
            if let Some(rest) = trimmed.strip_prefix("PREGAP ") {
                t.pregap_msf = Some(rest.trim().to_string());
            } else if let Some(rest) = trimmed.strip_prefix("POSTGAP ") {
                t.postgap_msf = Some(rest.trim().to_string());
            } else if trimmed.starts_with("INDEX 00") {
                t.has_index_00 = true;
            }
            // INDEX 01 / FILE lines: ignored (we recompute offsets per track).
        }
    }
    if let Some(prev) = current.take() {
        out.tracks.push(prev);
    }
    if out.tracks.is_empty() {
        bail!("no TRACK entries found in cue");
    }
    Ok(out)
}

/// Extract a single-track MODE1 CD CHD straight to ISO.
///
/// libchdman-rs's `cd::extract_to_iso` rejects multi-track or non-MODE1
/// CHDs — we surface a friendly error pointing at chd_to_bincue.
pub fn chd_to_iso(
    chd_path: &Path,
    iso_path: &Path,
    progress: Arc<Mutex<ConvertProgress>>,
) -> Result<()> {
    set_operation(&progress, "Converting CHD to ISO...");

    log(
        &progress,
        LogLevel::Info,
        format!(
            "Extracting CHD to ISO: {} -> {}",
            chd_path.display(),
            iso_path.display()
        ),
    );

    let progress_for_cb = Arc::clone(&progress);
    let mut on_progress = move |bytes_done: u64| {
        if let Ok(mut s) = progress_for_cb.lock() {
            s.current_bytes = bytes_done;
        }
    };

    libchdman_rs::cd::extract_to_iso(chd_path, iso_path, &mut on_progress).map_err(
        |e| match e {
            libchdman_rs::ChdError::UnsupportedFormat => {
                anyhow::anyhow!("CHD is multi-track or not MODE1 — extract to BIN/CUE instead")
            }
            other => anyhow::anyhow!("CD CHD extract to ISO failed: {:?}", other),
        },
    )?;

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
        to_chd(&iso_path, &chd_path, None, Arc::clone(&progress)).unwrap();
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
            None,
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
    fn test_chd_to_iso_native() {
        // Single-track MODE1 CHD -> ISO via the new libchdman-rs path.
        // Round-trip must be byte-equal against the source ISO.
        let dir = tempfile::tempdir().unwrap();
        let iso_path = dir.path().join("seed.iso");
        let mut iso_data = vec![0u8; 2048 * 48];
        for (i, b) in iso_data.iter_mut().enumerate() {
            *b = ((i * 23) ^ (i >> 4)) as u8;
        }
        std::fs::write(&iso_path, &iso_data).unwrap();

        let chd_path = dir.path().join("disc.chd");
        to_chd(
            &iso_path,
            &chd_path,
            None,
            Arc::new(Mutex::new(ConvertProgress::new())),
        )
        .unwrap();

        let restored = dir.path().join("restored.iso");
        chd_to_iso(
            &chd_path,
            &restored,
            Arc::new(Mutex::new(ConvertProgress::new())),
        )
        .unwrap();
        assert_eq!(std::fs::read(&restored).unwrap(), iso_data);
    }

    #[test]
    fn test_chd_to_bincue_native() {
        // BIN/CUE -> CHD -> BIN/CUE via the native pipeline. The
        // restored .bin must equal the original (chdman pads tracks to a
        // 4-frame boundary; our 32-sector seed is already aligned).
        let dir = tempfile::tempdir().unwrap();
        let iso_path = dir.path().join("seed.iso");
        let iso_data = vec![0xA5u8; 2048 * 32];
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
        let bin_in = std::fs::read(&bin_path).unwrap();

        let chd_path = dir.path().join("disc.chd");
        to_chd(
            &cue_path,
            &chd_path,
            None,
            Arc::new(Mutex::new(ConvertProgress::new())),
        )
        .unwrap();

        let out_cue = dir.path().join("out.cue");
        chd_to_bincue(
            &chd_path,
            &out_cue,
            Arc::new(Mutex::new(ConvertProgress::new())),
        )
        .unwrap();
        let out_bin = out_cue.with_extension("bin");
        assert!(out_bin.exists());
        assert_eq!(std::fs::read(&out_bin).unwrap(), bin_in);
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

    /// Multi-FILE cue (MODE1 data + AUDIO) -> CHD -> multi-bin extraction.
    ///
    /// Mirrors the layout of `libchdman-rs/target/test-fixtures/libchdman-rs-cd.cue`
    /// but built inline so we don't depend on that crate's test-fixture
    /// extraction. After round-trip we must end up with one .bin per track
    /// (named `<stem> (Track NN).bin`) and a multi-FILE cue.
    #[test]
    fn test_chd_to_bincue_multi_round_trip() {
        let dir = tempfile::tempdir().unwrap();

        // Track 1: MODE1 data via the iso_to_bincue helper (32 sectors, already
        // 4-frame aligned). Throw away its solo cue — we'll write our own.
        let iso_path = dir.path().join("seed.iso");
        let iso_data = {
            let mut v = vec![0u8; 2048 * 32];
            for (i, b) in v.iter_mut().enumerate() {
                *b = ((i * 17) ^ (i >> 2)) as u8;
            }
            v
        };
        std::fs::write(&iso_path, &iso_data).unwrap();
        let data_bin = dir.path().join("data.bin");
        let _scratch_cue = dir.path().join("scratch.cue");
        iso_to_bincue(
            &iso_path,
            &data_bin,
            &_scratch_cue,
            Arc::new(Mutex::new(ConvertProgress::new())),
        )
        .unwrap();
        let data_bin_size = std::fs::metadata(&data_bin).unwrap().len();
        assert_eq!(data_bin_size, 32 * 2352);

        // Track 2: AUDIO — 8 sectors (4-frame aligned) of raw bytes.
        let audio_bin = dir.path().join("audio.bin");
        let audio_data: Vec<u8> = (0..(8usize * 2352))
            .map(|i| ((i.wrapping_mul(53) ^ (i >> 1)) & 0xff) as u8)
            .collect();
        std::fs::write(&audio_bin, &audio_data).unwrap();

        // Multi-FILE cue.
        let cue_path = dir.path().join("disc.cue");
        std::fs::write(
            &cue_path,
            "FILE \"data.bin\" BINARY\n  \
             TRACK 01 MODE1/2352\n    \
             INDEX 01 00:00:00\n\
             FILE \"audio.bin\" BINARY\n  \
             TRACK 02 AUDIO\n    \
             INDEX 01 00:00:00\n",
        )
        .unwrap();

        // Encode to CHD (libchdman-rs handles multi-FILE input via parse_toc).
        let chd_path = dir.path().join("disc.chd");
        to_chd(
            &cue_path,
            &chd_path,
            None,
            Arc::new(Mutex::new(ConvertProgress::new())),
        )
        .unwrap();

        // Verify the CHD is multi-track.
        let chd = libchdman_rs::Chd::open(chd_path.to_str().unwrap(), false, None).unwrap();
        let tracks = libchdman_rs::cd::list_tracks(&chd).unwrap();
        assert_eq!(tracks.len(), 2, "expected 2-track CHD");
        drop(chd);

        // Extract via multi-bin.
        let out_cue = dir.path().join("out.cue");
        chd_to_bincue_multi(
            &chd_path,
            &out_cue,
            Arc::new(Mutex::new(ConvertProgress::new())),
        )
        .unwrap();

        // Per-track .bin files must exist with the expected names.
        let track1 = dir.path().join("out (Track 01).bin");
        let track2 = dir.path().join("out (Track 02).bin");
        assert!(track1.exists(), "Track 01 bin missing");
        assert!(track2.exists(), "Track 02 bin missing");

        // The single-bin alongside the cue must have been removed.
        let single = dir.path().join("out.bin");
        assert!(!single.exists(), "single-bin should have been removed");

        // Cue must have two FILE entries.
        let out_cue_text = std::fs::read_to_string(&out_cue).unwrap();
        let file_lines = out_cue_text
            .lines()
            .filter(|l| l.trim_start().starts_with("FILE "))
            .count();
        assert_eq!(file_lines, 2, "cue should have one FILE per track");
        assert!(out_cue_text.contains("(Track 01).bin"));
        assert!(out_cue_text.contains("(Track 02).bin"));

        // Sizes must equal frames * datasize per track. We don't reach for
        // exact byte-equality against the originals because libchdman-rs may
        // pad to 4-frame boundaries (we already aligned, so padding=0 here).
        let t1_size = std::fs::metadata(&track1).unwrap().len();
        let t2_size = std::fs::metadata(&track2).unwrap().len();
        assert_eq!(t1_size, u64::from(tracks[0].frames) * 2352);
        assert_eq!(t2_size, u64::from(tracks[1].frames) * 2352);
    }
}
