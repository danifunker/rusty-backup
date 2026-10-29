//! CD-DA audio for the Optical tab's in-app player: list a disc's audio tracks
//! and stream a track's PCM. Supports **CHD** and **BIN/CUE**; [`read_tracks`]
//! and [`extract_audio_pcm`] dispatch on the file's [`DiscFormat`].
//!
//! Both backings hand back the same little-endian, 2352-byte/sector CD-DA PCM:
//!
//! - **CHD**: [`libchdman_rs`]'s [`CdAudioReader`] (the copy we already link for
//!   the `chd` feature) decodes the requested track's sectors on demand, with the
//!   same bytes `chdman extractcd` would write: CHD's big-endian CD-DA swapped
//!   back to little-endian, 96-byte subcode dropped. No temp BIN, no whole-track
//!   buffer.
//! - **BIN/CUE**: the `.bin` is *already* little-endian 2352-B raw sectors laid
//!   out per the `.cue`, so there is nothing to decode. [`opticaldiscs::bincue`]
//!   parses the cue (track types, per-track BIN path + byte offset) and we read
//!   the audio track's sectors straight out of the `.bin` — including multi-BIN
//!   layouts, where each track names its own file.
//!
//! Both backings start a track at `INDEX 01`. A CHD often stores each track's
//! `INDEX 00` pregap (~2 s of silence) at the start of that track's sectors, so
//! the CHD path skips it; BIN/CUE already reads from `INDEX 01`. Neither goes
//! through cd-da-reader's `read_track`, whose CD-Extra trailing-gap rule is only
//! right for a physical disc.
//!
//! Ported from the ODE artwork-downloader's `src/disc/cd_audio.rs`.

use std::io::{Read, Seek, SeekFrom};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use cd_da_reader::{open_track_stream_at, AudioSectorReader};
use libchdman_rs::cd::{list_tracks, CdAudioReader, TrackType};
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
/// `on_samples` returns [`ControlFlow::Break`] to stop reading early (the user
/// hit Stop); that is not an error, and the count covers what was emitted.
pub fn extract_audio_pcm<F>(path: &Path, track_number: u32, on_samples: F) -> Result<u64, String>
where
    F: FnMut(&[i16]) -> ControlFlow<()>,
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
        .enumerate()
        .map(|(i, t)| CdTrack {
            number: t.track_num,
            track_type: format!("{:?}", t.track_type),
            // FRAMES counts a stored pregap; report INDEX 01 onward, like BIN/CUE.
            frames: t.frames.saturating_sub(stored_pregap(&chd, i)),
            pregap: t.pregap,
            is_audio: t.track_type == TrackType::Audio,
        })
        .collect();
    if tracks.is_empty() {
        return Err("no tracks in image".to_string());
    }
    Ok(tracks)
}

/// Stream a CHD audio track's PCM straight out of the CHD, ~1 s per callback.
fn extract_audio_pcm_chd<F>(
    path: &Path,
    track_number: u32,
    mut on_samples: F,
) -> Result<u64, String>
where
    F: FnMut(&[i16]) -> ControlFlow<()>,
{
    let chd = open_chd(path)?;
    let tracks = list_tracks(&chd).map_err(|e| format!("list CHD tracks: {e:?}"))?;
    // CdAudioReader takes a 0-based index into this list, not the track number.
    let index = tracks
        .iter()
        .position(|t| t.track_num == track_number)
        .ok_or_else(|| format!("no track {track_number} on this disc"))?;
    // CdAudioReader also reads raw data tracks; don't play one as "audio".
    if tracks[index].track_type != TrackType::Audio {
        return Err(format!("track {track_number} is not an AUDIO track"));
    }

    // open_track (not open) so a cooked data track elsewhere on the disc can't reject it.
    let pregap = stored_pregap(&chd, index);
    let reader = CdAudioReader::open_track(chd, index as u32)
        .map_err(|e| format!("open CHD track {track_number}: {e:?}"))?;
    let sectors = reader.sector_count();
    // Start at INDEX 01, as a CD player does, skipping any stored INDEX 00 pregap.
    let start = pregap.min(sectors);
    let src = ChdTrackAudio(Mutex::new(reader));
    let mut stream = open_track_stream_at(&src, start, sectors - start)
        .with_sectors_per_chunk(SECTORS_PER_SECOND);

    let mut total = 0u64;
    while let Some(chunk) = stream
        .next_chunk()
        .map_err(|e| format!("read track {track_number}: {e}"))?
    {
        let samples = pcm_le_i16(&chunk);
        total += samples.len() as u64;
        if on_samples(&samples).is_break() {
            break;
        }
    }
    Ok(total)
}

/// Frames of `INDEX 00` pregap the CHD stores at the start of track `index` (0-based).
/// MAME stores pregap data only when `PGTYPE` starts with `V`; legacy `CHTR` has none.
fn stored_pregap(chd: &Chd, index: usize) -> u32 {
    let record = [*b"CHT2", *b"CHGD"].iter().find_map(|tag| {
        chd.read_metadata(u32::from_be_bytes(*tag), index as u32)
            .ok()
    });
    let Some(record) = record else { return 0 };
    let text = String::from_utf8_lossy(&record);
    let field = |key: &str| {
        text.split_whitespace()
            .find_map(|kv| kv.strip_prefix(key))
            .map(|v| v.trim_end_matches('\0'))
    };
    match (field("PREGAP:"), field("PGTYPE:")) {
        (Some(n), Some(pgtype)) if pgtype.starts_with('V') => n.parse().unwrap_or(0),
        _ => 0,
    }
}

/// One CHD track exposed to `cd-da-reader`; the `Mutex` bridges its `&self` read to `&mut` sectors.
struct ChdTrackAudio(Mutex<CdAudioReader>);

impl AudioSectorReader for ChdTrackAudio {
    type Error = std::io::Error;

    fn read_audio_sectors(&self, start_lba: u32, count: u32) -> Result<Vec<u8>, Self::Error> {
        let mut buf = vec![0u8; count as usize * CdAudioReader::SECTOR_SIZE];
        self.0
            .lock()
            .map_err(|_| crate::compat::io_other("CHD reader poisoned"))?
            .read_sectors(start_lba, count, &mut buf)
            .map_err(|e| crate::compat::io_other(format!("read CHD sectors: {e:?}")))?;
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
/// already little-endian 2352-B raw PCM, so no decode — the track is exposed as an
/// [`AudioSectorReader`] and pulled through the same [`AudioTrackStream`] that
/// drives CHD playback.
fn extract_audio_pcm_bincue<F>(
    path: &Path,
    track_number: u32,
    mut on_samples: F,
) -> Result<u64, String>
where
    F: FnMut(&[i16]) -> ControlFlow<()>,
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

    // Expose the track's sectors to cd-da-reader as an AudioSectorReader (sector 0
    // = the track's first sector) so BIN/CUE and CHD share the same streaming
    // reader. The track's absolute LBA is irrelevant: the backing maps sector 0 to
    // `file_byte_offset`, so open_track_stream_at(.., 0, frames) covers exactly
    // this track — no TOC lookup and no CD-Extra rule, the same reason the CHD path
    // uses it.
    let reader = BinCueTrackReader {
        bin_path: track.bin_path.clone(),
        file_byte_offset: track.file_byte_offset,
    };
    let mut stream =
        open_track_stream_at(&reader, 0, frames).with_sectors_per_chunk(SECTORS_PER_SECOND);

    let mut total = 0u64;
    while let Some(chunk) = stream
        .next_chunk()
        .map_err(|e| format!("read track {track_number}: {e}"))?
    {
        let samples = pcm_le_i16(&chunk);
        total += samples.len() as u64;
        if on_samples(&samples).is_break() {
            break;
        }
    }
    Ok(total)
}

/// A single BIN/CUE audio track exposed to `cd-da-reader` via [`AudioSectorReader`]:
/// sector 0 is the track's first sector, mapped to `file_byte_offset` in `bin_path`.
///
/// Re-opens the BIN per read so the reader stays `&self`; one open per ~1 s chunk.
struct BinCueTrackReader {
    /// The `.bin` this track's sectors live in (each cue track names its own file,
    /// so multi-BIN layouts just point different tracks at different paths).
    bin_path: PathBuf,
    /// Byte offset of the track's first sector (sector 0) within `bin_path`.
    file_byte_offset: u64,
}

impl AudioSectorReader for BinCueTrackReader {
    type Error = std::io::Error;

    fn read_audio_sectors(&self, start_lba: u32, count: u32) -> Result<Vec<u8>, Self::Error> {
        let mut f = std::fs::File::open(&self.bin_path)?;
        f.seek(SeekFrom::Start(
            self.file_byte_offset + u64::from(start_lba) * BYTES_PER_SECTOR as u64,
        ))?;
        let mut buf = vec![0u8; count as usize * BYTES_PER_SECTOR];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }
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
