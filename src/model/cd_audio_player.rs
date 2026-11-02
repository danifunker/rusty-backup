//! CD-DA playback for the Optical tab's player: decode one audio track and play it.
//!
//! The rodio backend the egui player ran on a detached thread, now the body of a `model::job`
//! (`controller::optical::audio` spawns it). Decoding blocks for the whole track, so the job
//! queues ~1 s chunks with backpressure and stops within ~20 ms of a cancel. The device sink is
//! created and dropped on the job's thread.

use std::ops::ControlFlow;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rodio::buffer::SamplesBuffer;

use crate::model::job::JobCtx;
use crate::optical::cd_audio::{self, CDDA_CHANNELS, CDDA_SAMPLE_RATE};

pub use crate::optical::cd_audio::CdTrack;

/// How far ahead of the speaker the queue may run (each queued chunk is ~1 second).
const MAX_QUEUED_CHUNKS: usize = 4;

/// The audio tracks of a CHD or BIN/CUE image; `None` for anything else or an unreadable image.
pub fn read_tracks(path: &Path) -> Option<Vec<CdTrack>> {
    if !cd_audio::supports_path(path) {
        return None;
    }
    cd_audio::read_tracks(path).ok()
}

/// Wrap interleaved 16-bit CD-DA PCM as a rodio buffer (rodio mixes in f32).
fn cdda_buffer(samples: &[i16]) -> SamplesBuffer {
    let channels = std::num::NonZeroU16::new(CDDA_CHANNELS).unwrap_or(std::num::NonZeroU16::MIN);
    let rate = std::num::NonZeroU32::new(CDDA_SAMPLE_RATE).unwrap_or(std::num::NonZeroU32::MIN);
    SamplesBuffer::new(
        channels,
        rate,
        samples
            .iter()
            .map(|&s| rodio::Sample::from(s) / 32768.0)
            .collect::<Vec<_>>(),
    )
}

/// Play `track` of `image_path` until it ends or the job is cancelled; `started` is set when audio first reaches the sink.
pub fn play(
    image_path: &Path,
    track: u32,
    ctx: &JobCtx,
    started: &Arc<Mutex<Option<Instant>>>,
) -> Result<(), String> {
    let mut device = rodio::DeviceSinkBuilder::open_default_sink()
        .map_err(|e| format!("no audio output: {e}"))?;
    device.log_on_drop(false);
    let sink = rodio::Player::connect_new(device.mixer());

    let mut first = true;
    cd_audio::extract_audio_pcm(image_path, track, |samples| {
        if first {
            if let Ok(mut p) = started.lock() {
                *p = Some(Instant::now());
            }
            ctx.set_phase("Playing");
            first = false;
        }
        // Bounded queue: wait while the sink is a few chunks ahead; on Stop, cut it now.
        loop {
            if ctx.is_cancelled() {
                sink.stop();
                return ControlFlow::Break(());
            }
            if sink.len() < MAX_QUEUED_CHUNKS {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        sink.append(cdda_buffer(samples));
        ControlFlow::Continue(())
    })?;

    // Drain the queued audio, honouring Stop.
    loop {
        if ctx.is_cancelled() {
            sink.stop();
            break;
        }
        if sink.empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}
