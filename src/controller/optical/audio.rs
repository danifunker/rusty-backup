//! The Optical tab's CD-DA player: the loaded disc's audio tracks, the player window, and the
//! one track playing (a `model::job` over `model::cd_audio_player`, cancelled to stop).
//! Without the `audio` feature the player never has tracks and ignores every action.

#[cfg(feature = "audio")]
use std::path::{Path, PathBuf};
#[cfg(feature = "audio")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "audio")]
use std::time::Instant;

#[cfg(feature = "audio")]
use crate::controller::job_outcome;
#[cfg(feature = "audio")]
use crate::model::cd_audio_player;
#[cfg(feature = "audio")]
use crate::model::job::{Job, Notify};

#[cfg(feature = "audio")]
pub use crate::model::cd_audio_player::CdTrack;

/// Stable widget ids.
pub mod ids {
    pub const OPEN: &str = "optical.audio.open";
    pub const CLOSE: &str = "optical.audio.close";
    pub const PLAY: &str = "optical.audio.play";
    pub const STOP: &str = "optical.audio.stop";
}

/// Player intents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioAction {
    /// "Audio Tracks (N)": open the player window.
    Open,
    /// Close the window; playback stops with it.
    Close,
    Play(u32),
    Stop,
}

/// Where the playing track is.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PlayState {
    /// Opening the image and the audio device: no audio yet.
    Preparing,
    /// Audio is streaming; seconds since it started.
    Playing(f64),
}

/// The playing track as a shell shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Readout {
    pub track: u32,
    /// The line beside the "Audio Tracks" button.
    pub line: String,
    /// The player window's heading.
    pub heading: String,
    /// The player window's row for this track, and how far the track is; `None` while preparing.
    pub row: Option<String>,
    pub fraction: Option<f64>,
}

/// The playing track; dropping it stops playback.
#[cfg(feature = "audio")]
struct Playback {
    track: u32,
    job: Job<Result<(), String>>,
    started: Arc<Mutex<Option<Instant>>>,
}

#[cfg(feature = "audio")]
impl Drop for Playback {
    fn drop(&mut self) {
        self.job.cancel();
    }
}

#[cfg(feature = "audio")]
pub struct AudioPlayerController {
    notify: Arc<dyn Notify>,
    tracks: Option<Vec<CdTrack>>,
    loading: Option<Job<Option<Vec<CdTrack>>>>,
    path: Option<PathBuf>,
    playback: Option<Playback>,
    error: Option<String>,
    window_open: bool,
}

#[cfg(feature = "audio")]
impl AudioPlayerController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        AudioPlayerController {
            notify,
            tracks: None,
            loading: None,
            path: None,
            playback: None,
            error: None,
            window_open: false,
        }
    }

    /// Read a disc image's tracks in a job; a format without CD-DA leaves the player hidden.
    pub(super) fn set_disc(&mut self, path: &Path) {
        self.clear();
        let owned = path.to_path_buf();
        self.path = Some(owned.clone());
        self.loading = Some(Job::spawn(
            "Reading audio tracks...",
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_can_cancel(false);
                Ok(cd_audio_player::read_tracks(owned.as_path()))
            },
        ));
    }

    /// Drop the tracks, the window and any playback.
    pub(super) fn clear(&mut self) {
        self.tracks = None;
        self.loading = None;
        self.path = None;
        self.playback = None;
        self.error = None;
        self.window_open = false;
    }

    /// The disc has at least one audio track: the player is offered.
    pub fn has_audio(&self) -> bool {
        self.audio_tracks().next().is_some()
    }

    pub fn audio_tracks(&self) -> impl Iterator<Item = &CdTrack> {
        self.tracks.iter().flatten().filter(|t| t.is_audio)
    }

    pub fn window_open(&self) -> bool {
        self.window_open
    }

    /// Why the last playback failed, until the next one starts.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// The playing track and where it is.
    pub fn now_playing(&self) -> Option<(u32, PlayState)> {
        let pb = self.playback.as_ref()?;
        let started = pb.started.lock().ok().and_then(|s| *s);
        Some((
            pb.track,
            match started {
                Some(t) => PlayState::Playing(t.elapsed().as_secs_f64()),
                None => PlayState::Preparing,
            },
        ))
    }

    /// What every shell says about the playing track.
    pub fn readout(&self) -> Option<Readout> {
        let (track, state) = self.now_playing()?;
        Some(match state {
            PlayState::Preparing => Readout {
                track,
                line: format!("Playing track {track} - preparing..."),
                heading: format!("Track {track}: preparing..."),
                row: None,
                fraction: None,
            },
            PlayState::Playing(secs) => {
                let total = self.track_secs(track);
                let pos = secs.min(total);
                Readout {
                    track,
                    line: format!("Playing track {track} - {secs:.0}s"),
                    heading: format!("Playing track {track}   {pos:.0}s / {total:.0}s"),
                    row: Some(format!("Track {track} - {pos:.0}s")),
                    fraction: Some(if total > 0.0 { pos / total } else { 0.0 }),
                }
            }
        })
    }

    /// A track's length in seconds (75 frames a second).
    pub fn track_secs(&self, number: u32) -> f64 {
        self.tracks
            .iter()
            .flatten()
            .find(|t| t.number == number)
            .map(|t| f64::from(t.frames) / 75.0)
            .unwrap_or(0.0)
    }

    pub(super) fn tick(&mut self) {
        if let Some(found) = self.loading.as_ref().and_then(job_outcome) {
            self.loading = None;
            match found {
                Ok(Some(tracks)) => self.tracks = Some(tracks),
                // Not a readable CD image: keep the player hidden rather than show an error.
                _ => self.path = None,
            }
        }
        if let Some(done) = self.playback.as_ref().and_then(|pb| job_outcome(&pb.job)) {
            self.playback = None;
            match done {
                Ok(Err(e)) | Err(e) if e != "cancelled" => self.error = Some(e),
                _ => {}
            }
        }
    }

    pub(super) fn apply(&mut self, action: AudioAction) {
        match action {
            AudioAction::Open => {
                if self.has_audio() {
                    self.window_open = true;
                }
            }
            AudioAction::Close => {
                self.window_open = false;
                self.playback = None;
            }
            AudioAction::Stop => self.playback = None,
            AudioAction::Play(track) => {
                let Some(path) = self.path.clone() else {
                    return;
                };
                if self.playback.is_some() || !self.audio_tracks().any(|t| t.number == track) {
                    return;
                }
                self.error = None;
                let started = Arc::new(Mutex::new(None));
                let mark = Arc::clone(&started);
                let job = Job::spawn("Preparing", Arc::clone(&self.notify), move |ctx| {
                    Ok(cd_audio_player::play(path.as_path(), track, ctx, &mark))
                });
                self.playback = Some(Playback {
                    track,
                    job,
                    started,
                });
            }
        }
    }

    /// Block until the track list is read; for a headless shell or a test.
    pub fn wait_loaded(&mut self) {
        while self.loading.is_some() {
            self.tick();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}
