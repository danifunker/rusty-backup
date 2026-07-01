//! In-app CD-DA player for the Optical tab.
//!
//! Two pieces live here:
//!
//! - [`AudioPlayback`] — the rodio backend. Playing an audio track (see
//!   [`rusty_backup::optical::cd_audio`]) can be slow to start — a CHD extracts
//!   the whole disc to a temporary BIN first — so all the work runs on a
//!   dedicated thread. The UI polls [`AudioPlayback::state`] each frame and
//!   offers a Stop button. rodio's `OutputStream` is `!Send`, so it is created
//!   and owned entirely on the playback thread; the UI only ever touches the
//!   shared state flag.
//! - [`OpticalAudioPlayer`] — the tab-side widget: track list, active job,
//!   and the pop-out modal with per-track play/stop. Held as a field on
//!   `OpticalTab`; `set_disc` loads a disc's tracks and `ui` draws the player.
//!
//! ASCII-only labels throughout (the default egui font has no symbol glyphs).
//!
//! Ported from the ODE artwork-downloader's `src/gui/audio.rs` + the audio
//! player pieces of its `src/gui/app.rs`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rodio::buffer::SamplesBuffer;

use rusty_backup::optical::cd_audio::{self, CdTrack, CDDA_CHANNELS, CDDA_SAMPLE_RATE};

/// How far ahead of the speaker we let the queue run before applying
/// backpressure (each queued chunk is ~1 second).
const MAX_QUEUED_CHUNKS: usize = 4;

/// Lifecycle of a single track's playback.
#[derive(Clone, Debug, PartialEq)]
pub enum PlaybackState {
    /// Extracting the disc to a temp BIN — no audio yet.
    Preparing,
    /// Audio is streaming to the output device.
    Playing,
    /// Finished normally or stopped by the user.
    Finished,
    /// Failed; carries a human-readable message.
    Failed(String),
}

/// Handle to a background playback job for one audio track. Dropping it (or
/// calling [`AudioPlayback::stop`]) signals the worker to tear down.
pub struct AudioPlayback {
    track: u32,
    stop: Arc<AtomicBool>,
    state: Arc<Mutex<PlaybackState>>,
    /// Set to `Instant::now()` the moment the first audio reaches the sink, i.e.
    /// when playback actually starts. `None` while still `Preparing`.
    play_start: Arc<Mutex<Option<Instant>>>,
}

impl AudioPlayback {
    /// Spawn a worker that extracts and plays `track` from `image_path`.
    pub fn start(image_path: PathBuf, track: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let state = Arc::new(Mutex::new(PlaybackState::Preparing));
        let play_start = Arc::new(Mutex::new(None));
        let stop_bg = Arc::clone(&stop);
        let state_bg = Arc::clone(&state);
        let play_start_bg = Arc::clone(&play_start);

        // Detached: the worker owns its rodio stream and the temp extraction,
        // and tears itself down when it sees the stop flag or finishes.
        thread::spawn(move || run(image_path, track, &stop_bg, &state_bg, &play_start_bg));

        Self {
            track,
            stop,
            state,
            play_start,
        }
    }

    /// The track number this job is playing.
    pub fn track(&self) -> u32 {
        self.track
    }

    /// Current lifecycle state (cheap; clones a small enum).
    pub fn state(&self) -> PlaybackState {
        self.state
            .lock()
            .map(|s| s.clone())
            .unwrap_or(PlaybackState::Finished)
    }

    /// Seconds of audio played so far, or `None` while still preparing.
    ///
    /// CD-DA streams to the sink in real time, so wall-clock since the first
    /// sample was queued tracks the speaker position (bar a few ms of startup
    /// latency). Callers should clamp to the track length for display.
    pub fn elapsed_secs(&self) -> Option<f64> {
        self.play_start
            .lock()
            .ok()
            .and_then(|p| p.map(|t| t.elapsed().as_secs_f64()))
    }

    /// Signal the worker to stop. Extraction in progress stops at the next
    /// chunk boundary; queued audio is cut immediately.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl Drop for AudioPlayback {
    fn drop(&mut self) {
        self.stop();
    }
}

fn set(state: &Arc<Mutex<PlaybackState>>, value: PlaybackState) {
    if let Ok(mut s) = state.lock() {
        *s = value;
    }
}

fn run(
    image_path: PathBuf,
    track: u32,
    stop: &Arc<AtomicBool>,
    state: &Arc<Mutex<PlaybackState>>,
    play_start: &Arc<Mutex<Option<Instant>>>,
) {
    let (_stream, handle) = match rodio::OutputStream::try_default() {
        Ok(out) => out,
        Err(e) => {
            return set(
                state,
                PlaybackState::Failed(format!("no audio output: {e}")),
            )
        }
    };
    let sink = match rodio::Sink::try_new(&handle) {
        Ok(s) => s,
        Err(e) => return set(state, PlaybackState::Failed(format!("audio sink: {e}"))),
    };

    let mut started = false;
    let result = cd_audio::extract_audio_pcm(&image_path, track, |samples| {
        if !started {
            if let Ok(mut p) = play_start.lock() {
                *p = Some(Instant::now());
            }
            set(state, PlaybackState::Playing);
            started = true;
        }
        // Bounded queue: wait while the sink is a few chunks ahead, bailing if
        // the user hit Stop.
        while sink.len() >= MAX_QUEUED_CHUNKS {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        sink.append(SamplesBuffer::new(
            CDDA_CHANNELS,
            CDDA_SAMPLE_RATE,
            samples.to_vec(),
        ));
    });

    if let Err(e) = result {
        // A stop during extraction surfaces as a short-read/None, not a real
        // failure — report Finished in that case.
        if stop.load(Ordering::Relaxed) {
            set(state, PlaybackState::Finished);
        } else {
            set(state, PlaybackState::Failed(e));
        }
        return;
    }

    // Drain the queued audio, honoring Stop.
    loop {
        if stop.load(Ordering::Relaxed) {
            sink.stop();
            break;
        }
        if sink.empty() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    set(state, PlaybackState::Finished);
}

// -- Tab-side player widget ---------------------------------------------------

/// Optical-tab CD-DA player state + rendering. The tab holds one of these and
/// calls [`OpticalAudioPlayer::set_disc`] whenever the source changes and
/// [`OpticalAudioPlayer::ui`] once per frame.
#[derive(Default)]
pub struct OpticalAudioPlayer {
    /// Tracks for the loaded disc image; `None` until a supported disc loads.
    tracks: Option<Vec<CdTrack>>,
    /// Active playback job, if the user is playing a track.
    playback: Option<AudioPlayback>,
    /// Last playback failure, surfaced in the modal until the next play starts.
    error: Option<String>,
    /// Whether the pop-out player modal is open.
    modal_open: bool,
    /// The disc image path playback reads from.
    path: Option<PathBuf>,
}

impl OpticalAudioPlayer {
    /// Load audio-track metadata for a disc image. Clears any prior state first.
    /// A no-op for unsupported formats (device paths, ISO, etc.) — those simply
    /// leave the player hidden. Cheap: CHD reads only the TOC (no extraction);
    /// BIN/CUE parses the cue sheet.
    pub fn set_disc(&mut self, path: &Path) {
        self.clear();
        if !cd_audio::supports_path(path) {
            return;
        }
        match cd_audio::read_tracks(path) {
            Ok(tracks) => {
                self.tracks = Some(tracks);
                self.path = Some(path.to_path_buf());
            }
            // Not a readable CD image — leave the player hidden rather than
            // surfacing a scary error for, e.g., a data-only CHD.
            Err(_) => self.clear(),
        }
    }

    /// Drop all state (tracks, active job, modal). Stops playback via Drop.
    pub fn clear(&mut self) {
        self.tracks = None;
        self.playback = None;
        self.error = None;
        self.modal_open = false;
        self.path = None;
    }

    /// Whether the loaded disc has at least one audio track.
    fn has_audio(&self) -> bool {
        self.tracks
            .as_ref()
            .map(|t| t.iter().any(|t| t.is_audio))
            .unwrap_or(false)
    }

    /// Drop a finished/failed job, remembering a failure for the modal.
    fn reap(&mut self) {
        match self.playback.as_ref().map(|pb| pb.state()) {
            Some(PlaybackState::Failed(e)) => {
                self.error = Some(e);
                self.playback = None;
            }
            Some(PlaybackState::Finished) => {
                self.playback = None;
            }
            _ => {}
        }
    }

    /// Panel entry point: a button that opens the pop-out modal plus a compact
    /// now-playing readout. No-op for discs without audio tracks.
    pub fn ui(&mut self, ui: &mut egui::Ui) {
        if !self.has_audio() {
            return;
        }

        // Reap a finished/failed job so the play buttons re-enable.
        self.reap();

        let n_audio = self
            .tracks
            .as_ref()
            .map(|tracks| tracks.iter().filter(|t| t.is_audio).count())
            .unwrap_or(0);
        let now_playing = self
            .playback
            .as_ref()
            .map(|pb| (pb.track(), pb.state(), pb.elapsed_secs()));

        let mut open_modal = false;
        ui.horizontal(|ui| {
            ui.add_space(60.0);
            if ui
                .button(format!("Audio Tracks ({n_audio})"))
                .on_hover_text("Open the CD-DA player")
                .clicked()
            {
                open_modal = true;
            }
            if let Some((track, state, elapsed)) = now_playing.as_ref() {
                match state {
                    PlaybackState::Preparing => {
                        ui.label(format!("Playing track {track} - preparing..."));
                    }
                    PlaybackState::Playing => {
                        ui.label(format!(
                            "Playing track {track} - {:.0}s",
                            elapsed.unwrap_or(0.0)
                        ));
                    }
                    _ => {}
                }
            }
        });
        if open_modal {
            self.modal_open = true;
        }

        let ctx = ui.ctx().clone();
        self.render_modal(&ctx);

        // Keep repainting while a job is active so both the panel readout and the
        // modal's seconds counter stay live even when idle otherwise.
        if self.playback.is_some() {
            ctx.request_repaint_after(Duration::from_millis(200));
        }
    }

    /// The pop-out player modal: a multi-column track grid with per-track
    /// play/stop and a live elapsed-seconds readout for the active track.
    fn render_modal(&mut self, ctx: &egui::Context) {
        if !self.modal_open {
            return;
        }

        // Snapshot the active job so the closures below never hold a borrow of
        // `self.playback` across the UI build.
        let active = self
            .playback
            .as_ref()
            .map(|pb| (pb.track(), pb.state(), pb.elapsed_secs()));
        let busy = active.is_some();

        let mut to_play: Option<u32> = None;
        let mut do_stop = false;
        let mut close = false;

        let modal = egui::Modal::new(egui::Id::new("optical_audio_player_modal")).show(ctx, |ui| {
            ui.set_min_width(460.0);

            ui.horizontal(|ui| {
                ui.heading("Audio Tracks");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Close").clicked() {
                        close = true;
                    }
                });
            });

            // Now-playing banner: position in seconds + progress bar.
            if let Some((track, state, elapsed)) = active.as_ref() {
                let total = self
                    .tracks
                    .as_ref()
                    .and_then(|ts| ts.iter().find(|t| t.number == *track))
                    .map(|t| f64::from(t.frames) / 75.0)
                    .unwrap_or(0.0);
                match state {
                    PlaybackState::Preparing => {
                        ui.label(format!("Track {track}: preparing..."));
                    }
                    PlaybackState::Playing => {
                        let pos = elapsed.unwrap_or(0.0).min(total);
                        ui.label(
                            egui::RichText::new(format!(
                                "Playing track {track}   {pos:.0}s / {total:.0}s"
                            ))
                            .strong(),
                        );
                        let frac = if total > 0.0 {
                            (pos / total) as f32
                        } else {
                            0.0
                        };
                        ui.add(egui::ProgressBar::new(frac).desired_height(6.0));
                    }
                    _ => {}
                }
            }

            if let Some(err) = self.error.as_ref() {
                ui.colored_label(egui::Color32::RED, format!("Playback failed: {err}"));
            }

            ui.separator();

            // Multi-column grid of tracks.
            const COLS: usize = 3;
            egui::ScrollArea::vertical()
                .max_height(360.0)
                .show(ui, |ui| {
                    egui::Grid::new("optical_audio_tracks_grid")
                        .num_columns(COLS)
                        .spacing([18.0, 8.0])
                        .show(ui, |ui| {
                            let audio: Vec<&CdTrack> = self
                                .tracks
                                .as_ref()
                                .map(|ts| ts.iter().filter(|t| t.is_audio).collect())
                                .unwrap_or_default();
                            for (i, t) in audio.iter().enumerate() {
                                let is_this = active
                                    .as_ref()
                                    .map(|(n, _, _)| *n == t.number)
                                    .unwrap_or(false);
                                ui.horizontal(|ui| {
                                    if is_this {
                                        if ui.button("Stop").on_hover_text("Stop").clicked() {
                                            do_stop = true;
                                        }
                                    } else if ui
                                        .add_enabled(!busy, egui::Button::new("Play"))
                                        .clicked()
                                    {
                                        to_play = Some(t.number);
                                    }

                                    match active.as_ref() {
                                        Some((n, PlaybackState::Playing, elapsed))
                                            if *n == t.number =>
                                        {
                                            let total = f64::from(t.frames) / 75.0;
                                            let pos = elapsed.unwrap_or(0.0).min(total);
                                            ui.label(format!("Track {} - {pos:.0}s", t.number));
                                        }
                                        _ => {
                                            ui.label(format!(
                                                "Track {} ({})",
                                                t.number,
                                                t.duration_mmss()
                                            ));
                                        }
                                    }
                                });
                                if (i + 1) % COLS == 0 {
                                    ui.end_row();
                                }
                            }
                        });
                });
        });

        // Escape / click-outside also closes the modal.
        if modal.should_close() {
            close = true;
        }

        if do_stop {
            self.playback = None;
        }
        if let Some(track) = to_play {
            if let Some(path) = self.path.clone() {
                self.error = None;
                self.playback = Some(AudioPlayback::start(path, track));
            }
        }
        if close {
            self.modal_open = false;
            // Closing the player stops playback (dropping the job tears it down).
            self.playback = None;
        }
    }
}
