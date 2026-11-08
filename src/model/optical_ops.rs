//! The Optical tab's operations: detect a disc, rip a drive, rip to CHD, convert an image.
//!
//! The bodies the egui tab ran on its own worker threads, moved here unchanged so they write the
//! same files; `controller::optical` runs each as a `model::job`. `rb-cli optical rip` and
//! `rb-cli optical convert` call the same engine functions with their own output.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use opticaldiscs::detect::DiscImageInfo;
use opticaldiscs::formats::DiscFormat;

use crate::optical::convert::ConvertProgress;
use crate::optical::rip::{OpticalTarget, RipConfig, RipFormat, RipProgress};
use crate::rbformats::chd_options::ChdOptions;

/// What a rip or conversion writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Iso,
    BinCue,
    Chd,
}

impl OutputFormat {
    pub const ALL: [OutputFormat; 3] = [OutputFormat::Iso, OutputFormat::BinCue, OutputFormat::Chd];

    pub const fn label(self) -> &'static str {
        match self {
            OutputFormat::Iso => "ISO",
            OutputFormat::BinCue => "BIN/CUE",
            OutputFormat::Chd => "CHD",
        }
    }

    /// The output file's extension (a BIN/CUE output is named by its .cue).
    pub const fn extension(self) -> &'static str {
        match self {
            OutputFormat::Iso => "iso",
            OutputFormat::BinCue => "cue",
            OutputFormat::Chd => "chd",
        }
    }

    /// A conversion to this format makes sense from `source` (not to the format it already is).
    pub fn converts_from(self, source: Option<DiscFormat>) -> bool {
        let same = match self {
            OutputFormat::Iso => DiscFormat::Iso,
            OutputFormat::BinCue => DiscFormat::BinCue,
            OutputFormat::Chd => DiscFormat::Chd,
        };
        source != Some(same)
    }
}

/// Detect a disc's format and filesystem; a drive is read through the device-backed reader.
pub fn detect(path: &Path, physical: bool) -> Result<DiscImageInfo, String> {
    let probe = if physical {
        DiscImageInfo::open_physical(path)
    } else {
        DiscImageInfo::open(path)
    };
    probe.map_err(|e| e.to_string())
}

/// The output name the save picker suggests: the image's stem, else the volume label, else "disc".
pub fn default_output_name(
    image: Option<&Path>,
    volume_label: Option<&str>,
    format: OutputFormat,
) -> String {
    let stem = if let Some(path) = image {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("disc")
            .to_string()
    } else if let Some(label) = volume_label {
        label.to_string()
    } else {
        "disc".to_string()
    };
    format!("{stem}.{}", format.extension())
}

/// The files a run to `output` writes, the output first: a BIN/CUE is a .cue and its .bin.
pub fn written_files(output: &Path, format: OutputFormat, from_chd: bool) -> Vec<PathBuf> {
    let mut files = vec![output.to_path_buf()];
    if format != OutputFormat::BinCue {
        return files;
    }
    files.push(output.with_extension("bin"));
    // A CHD of several tracks splits its .bin into one file each.
    if let (true, Some(stem)) = (from_chd, output.file_stem().and_then(|s| s.to_str())) {
        let parent = output.parent().unwrap_or_else(|| Path::new("."));
        files.extend((1..=99).map(|n| parent.join(format!("{stem} (Track {n:02}).bin"))));
    }
    files
}

/// The files a conversion reads: the image, and the other half of a BIN/CUE.
pub fn source_files(image: &Path, source: Option<DiscFormat>) -> Vec<PathBuf> {
    let mut files = vec![image.to_path_buf()];
    if source == Some(DiscFormat::BinCue) {
        files.push(image.with_extension("cue"));
        files.push(image.with_extension("bin"));
    }
    files
}

fn same_file(a: &Path, b: &Path) -> bool {
    a == b
        || match (a.canonicalize(), b.canonicalize()) {
            (Ok(a), Ok(b)) => a == b,
            _ => false,
        }
}

/// Why `output` cannot be written: `File::create` truncates, so a source or a device must not get here.
pub fn output_refusal(output: &Path, format: OutputFormat, sources: &[PathBuf]) -> Option<String> {
    let is_bin = output
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("bin"));
    if format == OutputFormat::BinCue && is_bin {
        return Some("A BIN/CUE output is named by its .cue, not its .bin.".to_string());
    }
    for file in written_files(output, format, false) {
        if std::fs::metadata(&file).is_ok_and(|m| !m.is_file()) {
            return Some(format!("{} is not a regular file.", file.display()));
        }
        if sources.iter().any(|s| same_file(s, &file)) {
            return Some(format!(
                "{} is the source image; choose another output file.",
                file.display()
            ));
        }
    }
    None
}

/// Rip `target` to an ISO or BIN/CUE.
pub fn rip(
    target: OpticalTarget,
    output_path: PathBuf,
    format: OutputFormat,
    eject_after: bool,
    progress: Arc<Mutex<RipProgress>>,
) -> anyhow::Result<()> {
    let format = match format {
        OutputFormat::Iso => RipFormat::Iso,
        OutputFormat::BinCue => RipFormat::BinCue,
        OutputFormat::Chd => anyhow::bail!("a CHD rip goes through rip_to_chd"),
    };
    let config = RipConfig {
        device: target,
        output_path,
        format,
        eject_after,
    };
    crate::optical::run_rip(config, progress)
}

/// Dispatch to the conversion for this source and output format.
pub fn run_conversion(
    input_path: &Path,
    output_path: &Path,
    source_format: Option<DiscFormat>,
    output_format: OutputFormat,
    chd_options: Option<ChdOptions>,
    progress: Arc<Mutex<ConvertProgress>>,
) -> anyhow::Result<()> {
    use crate::optical::convert;

    match (source_format, output_format) {
        (Some(DiscFormat::Iso), OutputFormat::BinCue) => {
            let bin_path = output_path.with_extension("bin");
            convert::iso_to_bincue(input_path, &bin_path, output_path, progress)
        }
        (Some(DiscFormat::BinCue), OutputFormat::Iso) => {
            convert::bincue_to_iso(&cue_of(input_path), output_path, progress)
        }
        #[cfg(feature = "chd")]
        (Some(DiscFormat::Iso), OutputFormat::Chd) => {
            convert::to_chd(input_path, output_path, chd_options, progress)
        }
        #[cfg(feature = "chd")]
        (Some(DiscFormat::BinCue), OutputFormat::Chd) => {
            convert::to_chd(&cue_of(input_path), output_path, chd_options, progress)
        }
        #[cfg(feature = "chd")]
        (Some(DiscFormat::Chd), OutputFormat::BinCue) => {
            convert::chd_to_bincue(input_path, output_path, progress)
        }
        #[cfg(feature = "chd")]
        (Some(DiscFormat::Chd), OutputFormat::Iso) => {
            convert::chd_to_iso(input_path, output_path, progress)
        }
        _ => {
            let _ = chd_options;
            anyhow::bail!(
                "Unsupported conversion: {:?} -> {:?}",
                source_format,
                output_format
            );
        }
    }
}

/// A BIN/CUE input given as its .bin resolves to the .cue beside it.
fn cue_of(input_path: &Path) -> PathBuf {
    if input_path
        .extension()
        .map(|e| e.eq_ignore_ascii_case("bin"))
        .unwrap_or(false)
    {
        input_path.with_extension("cue")
    } else {
        input_path.to_path_buf()
    }
}

/// The Cancel flag as a probe the relay can poll.
#[cfg(feature = "chd")]
fn cancel_probe(progress: &Arc<Mutex<ConvertProgress>>) -> impl Fn() -> bool + Send + 'static {
    let from = Arc::clone(progress);
    move || from.lock().map(|p| p.cancel_requested).unwrap_or(false)
}

/// Rip a disc (local or remote) to a temporary BIN/CUE beside `chd_path`, encode it here, clean up.
#[cfg(feature = "chd")]
pub fn rip_to_chd(
    target: OpticalTarget,
    chd_path: &Path,
    eject_after: bool,
    chd_options: ChdOptions,
    progress: &Arc<Mutex<ConvertProgress>>,
) -> anyhow::Result<()> {
    use crate::backup::{LogLevel, LogMessage};
    use crate::optical::{convert, rip};

    let parent = chd_path.parent().unwrap_or_else(|| Path::new("."));
    let temp_cue = parent.join(".rusty-backup-rip-temp.cue");
    let temp_bin = parent.join(".rusty-backup-rip-temp.bin");

    let rip_config = RipConfig {
        device: target,
        output_path: temp_cue.clone(),
        format: RipFormat::BinCue,
        eject_after,
    };

    if let Ok(mut p) = progress.lock() {
        p.operation = "Ripping to temporary BIN/CUE...".into();
    }

    // The rip has its own progress; Cancel lands on `progress`, so forward it.
    let rip_progress = Arc::new(Mutex::new(rip::RipProgress::new()));
    let rip_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let forwarder =
        crate::model::worker::forward_cancel(cancel_probe(progress), Arc::clone(&rip_done), {
            let to = Arc::clone(&rip_progress);
            move || {
                if let Ok(mut r) = to.lock() {
                    r.cancel_requested = true;
                }
            }
        });
    let rip_result = rip::run_rip(rip_config, Arc::clone(&rip_progress));
    rip_done.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = forwarder.join();
    rip_result?;

    if let Ok(mut rp) = rip_progress.lock() {
        while let Some(msg) = rp.log_messages.pop_front() {
            if let Ok(mut p) = progress.lock() {
                p.log_messages.push_back(msg);
            }
        }
    }

    if let Ok(mut p) = progress.lock() {
        p.log_messages.push_back(LogMessage {
            level: LogLevel::Info,
            message: "Rip complete, converting to CHD...".into(),
        });
    }

    let convert_progress = Arc::new(Mutex::new(ConvertProgress::new()));
    let convert_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let forwarder =
        crate::model::worker::forward_cancel(cancel_probe(progress), Arc::clone(&convert_done), {
            let to = Arc::clone(&convert_progress);
            move || {
                if let Ok(mut c) = to.lock() {
                    c.cancel_requested = true;
                }
            }
        });
    let convert_result = convert::to_chd(
        &temp_cue,
        chd_path,
        Some(chd_options),
        Arc::clone(&convert_progress),
    );
    convert_done.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = forwarder.join();

    if let Ok(mut cp) = convert_progress.lock() {
        while let Some(msg) = cp.log_messages.pop_front() {
            if let Ok(mut p) = progress.lock() {
                p.log_messages.push_back(msg);
            }
        }
    }

    // Clean up temp files regardless of result.
    let _ = std::fs::remove_file(&temp_cue);
    let _ = std::fs::remove_file(&temp_bin);

    convert_result?;

    if let Ok(mut p) = progress.lock() {
        p.finished = true;
    }
    Ok(())
}

/// Without the `chd` feature there is no encoder.
#[cfg(not(feature = "chd"))]
pub fn rip_to_chd(
    _target: OpticalTarget,
    _chd_path: &Path,
    _eject_after: bool,
    _chd_options: ChdOptions,
    _progress: &Arc<Mutex<ConvertProgress>>,
) -> anyhow::Result<()> {
    anyhow::bail!("CHD output needs the `chd` feature; this binary was built without it")
}

/// The CHD Info text for a CHD image.
pub fn chd_info(path: &Path) -> Result<String, String> {
    crate::rbformats::chd::format_chd_info(path).map_err(|e| e.to_string())
}

/// Eject the disc in a local drive.
pub fn eject(device_path: &Path) -> Result<(), String> {
    crate::optical::source::eject_disc(device_path).map_err(|e| format!("{e:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_output_name_prefers_the_image_stem_then_the_label() {
        let img = Path::new("/x/Game Disc.bin");
        assert_eq!(
            default_output_name(Some(img), Some("VOL"), OutputFormat::Chd),
            "Game Disc.chd"
        );
        assert_eq!(
            default_output_name(None, Some("VOL"), OutputFormat::BinCue),
            "VOL.cue"
        );
        assert_eq!(
            default_output_name(None, None, OutputFormat::Iso),
            "disc.iso"
        );
    }

    #[test]
    fn a_conversion_is_never_to_the_source_format() {
        assert!(!OutputFormat::Iso.converts_from(Some(DiscFormat::Iso)));
        assert!(OutputFormat::Chd.converts_from(Some(DiscFormat::Iso)));
        assert!(OutputFormat::BinCue.converts_from(None));
    }
}
