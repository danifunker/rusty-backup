//! CD-DA audio for the Optical tab's in-app player: list a disc's audio tracks
//! and stream a track's PCM. Supports **CHD** and **BIN/CUE**; [`read_tracks`]
//! and [`extract_audio_pcm`] dispatch on the file's [`DiscFormat`].
//!
//! Both backings hand back the same little-endian, 2352-byte/sector CD-DA PCM:
//!
//! - **CHD**: decoded with [`libchdman_rs`] (the copy we already link for the
//!   `chd` feature) to a temporary redump-style BIN via [`extract_to_cue`] —
//!   which byte-swaps CHD's big-endian CD-DA back to little-endian and strips the
//!   96-byte subcode — then sliced per sector.
//! - **BIN/CUE**: the `.bin` is *already* little-endian 2352-B raw sectors laid
//!   out per the `.cue`, so there is nothing to decode. [`opticaldiscs::bincue`]
//!   parses the cue (track types, per-track BIN path + byte offset) and we read
//!   the audio track's sectors straight out of the `.bin` — including multi-BIN
//!   layouts, where each track names its own file.
//!
//! For CHD we resolve track bounds from a cumulative TOC rather than
//! cd-da-reader's `read_track`, whose CD-Extra trailing-gap subtraction (correct
//! for a physical disc) would drop real audio from a gapless extracted BIN.
//!
//! Ported from the ODE artwork-downloader's `src/disc/cd_audio.rs`.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use cd_da_reader::{lba_to_msf, AudioSectorReader, Toc, Track};
use libchdman_rs::cd::{extract_to_cue, list_tracks, TrackType};
use libchdman_rs::Chd;
use opticaldiscs::bincue::{self, BinTrack};
use opticaldiscs::formats::DiscFormat;

/// CD-DA sample rate (Hz).
pub const CDDA_SAMPLE_RATE: u32 = 44_100;
/// CD-DA channel count (stereo).
pub const CDDA_CHANNELS: u16 = 2;

/// Raw CD sector size: 2352 bytes = 588 interleaved 16-bit stereo sample-frames.
const BYTES_PER_SECTOR: usize = 2352;
/// One second of CD audio is 75 sectors.
const SECTORS_PER_SECOND: u32 = 75;

/// One CD track for the UI, from either a CHD or a BIN/CUE image.
#[derive(Debug, Clone)]
pub struct CdTrack {
    /// 1-based track number.
    pub number: u32,
    /// Track type label, e.g. `Audio`, `Mode1`.
    pub track_type: String,
    /// Track length in frames (sectors), excluding pregap.
    pub frames: u32,
    /// Pregap length in frames (0 when the backing doesn't expose it).
    pub pregap: u32,
    /// Whether this is a CD-DA (audio) track.
    pub is_audio: bool,
}

impl CdTrack {
    /// Track length formatted as `MM:SS` (75 frames/sec).
    pub fn duration_mmss(&self) -> String {
        let secs = self.frames / 75;
        format!("{:02}:{:02}", secs / 60, secs % 60)
    }
}

/// Whether this path is a format we can list / play CD-DA from (CHD or BIN/CUE).
/// Device paths and data-only image formats return `false`.
pub fn supports_path(path: &Path) -> bool {
    matches!(
        DiscFormat::from_path(path),
        Some(DiscFormat::Chd) | Some(DiscFormat::BinCue)
    )
}

/// List a disc's CD tracks. Supports CHD and BIN/CUE; errors for other formats.
pub fn read_tracks(path: &Path) -> Result<Vec<CdTrack>, String> {
    match DiscFormat::from_path(path) {
        Some(DiscFormat::Chd) => read_tracks_chd(path),
        Some(DiscFormat::BinCue) => read_tracks_bincue(path),
        other => Err(format!(
            "no CD-DA support for {other:?} ({})",
            path.display()
        )),
    }
}

/// Stream an audio track's PCM, invoking `on_samples` with batches of
/// interleaved little-endian `i16` stereo samples, and return the total number
/// of `i16` samples emitted. Errors if the requested track isn't an audio track.
///
/// Dispatches on format: CHD extracts the disc once up front (the slow
/// "Preparing" cost) then slices; BIN/CUE streams straight from the `.bin`.
pub fn extract_audio_pcm<F>(path: &Path, track_number: u32, on_samples: F) -> Result<u64, String>
where
    F: FnMut(&[i16]),
{
    match DiscFormat::from_path(path) {
        Some(DiscFormat::Chd) => extract_audio_pcm_chd(path, track_number, on_samples),
        Some(DiscFormat::BinCue) => extract_audio_pcm_bincue(path, track_number, on_samples),
        other => Err(format!(
            "no CD-DA support for {other:?} ({})",
            path.display()
        )),
    }
}

/// Convert little-endian PCM bytes to interleaved `i16` samples, never relying
/// on host endianness.
fn pcm_le_i16(bytes: &[u8]) -> Vec<i16> {
    bytes
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect()
}

// -- CHD backing --------------------------------------------------------------

/// Open a CHD CD image and return its track list.
fn read_tracks_chd(path: &Path) -> Result<Vec<CdTrack>, String> {
    let chd = open_chd(path)?;
    let tracks: Vec<CdTrack> = list_tracks(&chd)
        .map_err(|e| format!("list CHD tracks: {e:?}"))?
        .into_iter()
        .map(|t| CdTrack {
            number: t.track_num,
            track_type: format!("{:?}", t.track_type),
            frames: t.frames,
            pregap: t.pregap,
            is_audio: t.track_type == TrackType::Audio,
        })
        .collect();
    if tracks.is_empty() {
        return Err("no tracks in image".to_string());
    }
    Ok(tracks)
}

/// Stream a CHD audio track's PCM. Extracts the whole disc to a temp BIN, then
/// reads the track's sector range in one shot and emits it in ~1 s batches.
fn extract_audio_pcm_chd<F>(
    path: &Path,
    track_number: u32,
    mut on_samples: F,
) -> Result<u64, String>
where
    F: FnMut(&[i16]),
{
    // ChdDisc::open is the slow "Preparing" cost (whole-disc extract); the
    // per-track read below is fast.
    let disc = ChdDisc::open(path)?;

    // Guard against reading a data track's bytes as "audio".
    let idx = disc
        .toc
        .tracks
        .iter()
        .position(|t| u32::from(t.number) == track_number)
        .ok_or_else(|| format!("no track {track_number} on this disc"))?;
    let track = &disc.toc.tracks[idx];
    if !track.is_audio {
        return Err(format!("track {track_number} is not an AUDIO track"));
    }

    // Bounds come straight from our cumulative TOC: the extracted BIN is gapless
    // (each track is exactly `frames` sectors, back-to-back), so a track spans
    // from its own `start_lba` to the next track's (or the leadout).
    //
    // We deliberately do NOT use cd-da-reader's `read_track` here. Its
    // `get_track_bounds` applies a CD-Extra rule that subtracts a fixed
    // 11,400-sector inter-session gap from the last audio track before a data
    // track. That gap is real on a physical disc but is absent from a
    // CHD-derived image, so honouring it would chop ~2.5 min of real audio off
    // that track. Every other track reads identically to `read_track`.
    let start_lba = track.start_lba;
    let end_lba = disc
        .toc
        .tracks
        .get(idx + 1)
        .map(|t| t.start_lba)
        .unwrap_or(disc.toc.leadout_lba);
    if end_lba <= start_lba {
        return Err(format!("track {track_number}: bad TOC bounds"));
    }
    let pcm = disc
        .read_audio_sectors(start_lba, end_lba - start_lba)
        .map_err(|e| format!("read track {track_number}: {e}"))?;

    // Batch ~1 second of audio per callback: 1176 i16 per CD frame x 75 frames/s.
    const BYTES_PER_BATCH: usize = 1176 * 75 * 2;
    let mut total = 0u64;
    for chunk in pcm.chunks(BYTES_PER_BATCH) {
        let samples = pcm_le_i16(chunk);
        total += samples.len() as u64;
        on_samples(&samples);
    }
    Ok(total)
}

/// A CHD CD image extracted to a temporary redump-style BIN, exposing its audio
/// sectors to `cd-da-reader` via [`AudioSectorReader`].
struct ChdDisc {
    /// Temp BIN written by [`extract_to_cue`]: little-endian, 2352 B/sector,
    /// tracks laid out back-to-back.
    bin_path: PathBuf,
    /// TOC describing where each track's sectors live in `bin_path`.
    toc: Toc,
    /// Owns the temp dir; dropping it deletes the extracted BIN/CUE.
    _tmp: tempfile::TempDir,
}

impl ChdDisc {
    /// Extract `chd` to a temp BIN and build the matching `cd-da-reader` TOC.
    fn open(chd: &Path) -> Result<Self, String> {
        let handle = open_chd(chd)?;
        let tracks = list_tracks(&handle).map_err(|e| format!("list CHD tracks: {e:?}"))?;
        if tracks.is_empty() {
            return Err("no tracks in image".to_string());
        }

        // Extract the whole disc to a temp BIN. extract_to_cue already yields
        // little-endian 2352-B sectors with subcode stripped, so ChdDisc only
        // has to slice bytes.
        let tmp = tempfile::Builder::new()
            .prefix("rb-chd-audio-")
            .tempdir()
            .map_err(|e| format!("create temp dir: {e}"))?;
        let bin_path = tmp.path().join("disc.bin");
        let cue_path = tmp.path().join("disc.cue");
        let mut on_progress = |_written: u64| {};
        extract_to_cue(chd, &cue_path, &bin_path, &mut on_progress)
            .map_err(|e| format!("extract CHD to BIN: {e:?}"))?;

        // Build cd-da-reader's TOC from cumulative stored-frame offsets, which is
        // exactly extract_to_cue's single-BIN layout: each track occupies
        // `frames` sectors of 2352 bytes, back-to-back, so a track's absolute
        // sector index (start_lba) doubles as its BIN byte offset / 2352.
        //
        // NB: this assumes the common raw-2352 CHD (no chdman pad/split frames
        // and no cooked data tracks). If a disc ever sounds wrong, diff a dumped
        // WAV against `chdman extractcd` and reconcile against the `.cue` that
        // extract_to_cue just wrote.
        let mut cd_tracks = Vec::with_capacity(tracks.len());
        let mut lba = 0u32;
        for t in &tracks {
            cd_tracks.push(Track {
                number: t.track_num as u8, // CDs have <= 99 tracks
                start_lba: lba,
                start_msf: lba_to_msf(lba),
                is_audio: t.track_type == TrackType::Audio,
            });
            lba = lba.saturating_add(t.frames);
        }
        let first_track = tracks.iter().map(|t| t.track_num as u8).min().unwrap_or(1);
        let last_track = tracks.iter().map(|t| t.track_num as u8).max().unwrap_or(1);
        let toc = Toc {
            first_track,
            last_track,
            tracks: cd_tracks,
            leadout_lba: lba,
        };

        Ok(Self {
            bin_path,
            toc,
            _tmp: tmp,
        })
    }
}

impl AudioSectorReader for ChdDisc {
    type Error = std::io::Error;

    fn read_audio_sectors(&self, start_lba: u32, count: u32) -> Result<Vec<u8>, Self::Error> {
        let mut f = std::fs::File::open(&self.bin_path)?;
        f.seek(SeekFrom::Start(
            u64::from(start_lba) * BYTES_PER_SECTOR as u64,
        ))?;
        let mut buf = vec![0u8; count as usize * BYTES_PER_SECTOR];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }
}

/// Open a CHD read-only, mapping the path/CHD errors to a message.
fn open_chd(path: &Path) -> Result<Chd, String> {
    let path_str = path
        .to_str()
        .ok_or_else(|| format!("non-UTF-8 path: {}", path.display()))?;
    Chd::open(path_str, false, None).map_err(|e| format!("open CHD: {e:?}"))
}

// -- BIN/CUE backing ----------------------------------------------------------

/// Parse a `.cue` (or the `.cue` sibling of a `.bin`) into a track list.
fn read_tracks_bincue(path: &Path) -> Result<Vec<CdTrack>, String> {
    let cue = resolve_cue(path)?;
    let tracks = bincue::parse_cue_tracks(&cue).map_err(|e| format!("parse cue: {e}"))?;
    let mut out = Vec::with_capacity(tracks.len());
    for t in &tracks {
        out.push(CdTrack {
            number: t.track_no,
            track_type: format!("{:?}", t.track_type),
            frames: bincue_frames(t)?,
            pregap: 0,
            is_audio: t.track_type == bincue::TrackType::Audio,
        });
    }
    if out.is_empty() {
        return Err("no tracks in cue".to_string());
    }
    Ok(out)
}

/// Stream a BIN/CUE audio track's PCM straight from the `.bin`. Audio sectors are
/// already little-endian 2352-B raw PCM, so no decode — just seek and read.
fn extract_audio_pcm_bincue<F>(
    path: &Path,
    track_number: u32,
    mut on_samples: F,
) -> Result<u64, String>
where
    F: FnMut(&[i16]),
{
    let cue = resolve_cue(path)?;
    let tracks = bincue::parse_cue_tracks(&cue).map_err(|e| format!("parse cue: {e}"))?;
    let track = tracks
        .iter()
        .find(|t| t.track_no == track_number)
        .ok_or_else(|| format!("no track {track_number} on this disc"))?;
    if track.track_type != bincue::TrackType::Audio {
        return Err(format!("track {track_number} is not an AUDIO track"));
    }
    let frames = bincue_frames(track)?;

    let mut f = std::fs::File::open(&track.bin_path)
        .map_err(|e| format!("open {}: {e}", track.bin_path.display()))?;
    f.seek(SeekFrom::Start(track.file_byte_offset))
        .map_err(|e| format!("seek {}: {e}", track.bin_path.display()))?;

    // Emit ~1 s (75 sectors) per callback so the player starts quickly and never
    // holds the whole track in memory.
    let mut buf = vec![0u8; SECTORS_PER_SECOND as usize * BYTES_PER_SECTOR];
    let mut remaining = frames;
    let mut total = 0u64;
    while remaining > 0 {
        let n = remaining.min(SECTORS_PER_SECOND);
        let bytes = n as usize * BYTES_PER_SECTOR;
        f.read_exact(&mut buf[..bytes])
            .map_err(|e| format!("read track {track_number}: {e}"))?;
        let samples = pcm_le_i16(&buf[..bytes]);
        total += samples.len() as u64;
        on_samples(&samples);
        remaining -= n;
    }
    Ok(total)
}

/// Frames (sectors) in a BIN/CUE track. opticaldiscs derives this from adjacent
/// INDEX 01 positions, but returns 0 for the last track in a BIN file (nothing
/// after it to measure against); fall back to the BIN file's size there.
fn bincue_frames(t: &BinTrack) -> Result<u32, String> {
    if t.frame_count > 0 {
        return Ok(t.frame_count as u32);
    }
    let len = std::fs::metadata(&t.bin_path)
        .map_err(|e| format!("stat {}: {e}", t.bin_path.display()))?
        .len();
    let avail = len.saturating_sub(t.file_byte_offset);
    Ok((avail / t.sector_size()) as u32)
}

/// Resolve a BIN/CUE path to its `.cue`: pass a `.cue` through, or find the
/// `.cue` sitting next to a `.bin`.
fn resolve_cue(path: &Path) -> Result<PathBuf, String> {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("cue") => Ok(path.to_path_buf()),
        Some("bin") => {
            let cue = path.with_extension("cue");
            if cue.exists() {
                Ok(cue)
            } else {
                Err(format!("no .cue sheet found next to {}", path.display()))
            }
        }
        _ => Err(format!("not a BIN/CUE path: {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_is_mmss() {
        let t = CdTrack {
            number: 1,
            track_type: "Audio".to_string(),
            frames: 75 * 90, // 90 seconds
            pregap: 0,
            is_audio: true,
        };
        assert_eq!(t.duration_mmss(), "01:30");
    }

    #[test]
    fn supports_only_chd_and_bincue() {
        assert!(supports_path(Path::new("disc.chd")));
        assert!(supports_path(Path::new("disc.cue")));
        assert!(supports_path(Path::new("disc.bin")));
        assert!(!supports_path(Path::new("disc.iso")));
        assert!(!supports_path(Path::new("/dev/sr0")));
    }
}
