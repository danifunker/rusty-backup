//! The Optical tab's CD-DA player: draws `controller::optical::audio` (the "Audio Tracks (N)"
//! button with a now-playing line, and the player window's track grid) and returns the frame's
//! `AudioAction`s. ASCII-only labels (the default egui font has no symbol glyphs).

use std::time::Duration;

use rusty_backup::controller::optical::{AudioAction, OpticalAction, OpticalController, PlayState};

/// Draw the player for a disc with audio tracks; nothing otherwise.
pub fn show(ui: &mut egui::Ui, ctl: &OpticalController, out: &mut Vec<OpticalAction>) {
    let player = ctl.audio();
    if !player.has_audio() {
        return;
    }
    let mut actions = Vec::new();
    let n_audio = player.audio_tracks().count();
    let now = player.now_playing();
    ui.horizontal(|ui| {
        ui.add_space(60.0);
        if ui
            .button(format!("Audio Tracks ({n_audio})"))
            .on_hover_text("Open the CD-DA player")
            .clicked()
        {
            actions.push(AudioAction::Open);
        }
        match now {
            Some((track, PlayState::Preparing)) => {
                ui.label(format!("Playing track {track} - preparing..."));
            }
            Some((track, PlayState::Playing(secs))) => {
                ui.label(format!("Playing track {track} - {secs:.0}s"));
            }
            None => {}
        }
    });

    if player.window_open() {
        window(ui.ctx(), ctl, &mut actions);
    }
    // Keep the readouts live while a track plays.
    if now.is_some() {
        ui.ctx().request_repaint_after(Duration::from_millis(200));
    }
    out.extend(actions.into_iter().map(OpticalAction::Audio));
}

/// The player window: the playing track's position and a grid of tracks with Play / Stop.
fn window(ctx: &egui::Context, ctl: &OpticalController, actions: &mut Vec<AudioAction>) {
    let player = ctl.audio();
    let now = player.now_playing();
    let busy = now.is_some();
    let modal = egui::Modal::new(egui::Id::unique("optical_audio_player_modal")).show(ctx, |ui| {
        ui.set_min_width(460.0);
        ui.horizontal(|ui| {
            ui.heading("Audio Tracks");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Close").clicked() {
                    actions.push(AudioAction::Close);
                }
            });
        });

        match now {
            Some((track, PlayState::Preparing)) => {
                ui.label(format!("Track {track}: preparing..."));
            }
            Some((track, PlayState::Playing(secs))) => {
                let total = player.track_secs(track);
                let pos = secs.min(total);
                ui.label(
                    egui::RichText::new(format!("Playing track {track}   {pos:.0}s / {total:.0}s"))
                        .strong(),
                );
                let frac = if total > 0.0 {
                    (pos / total) as f32
                } else {
                    0.0
                };
                ui.add(egui::ProgressBar::new(frac).desired_height(6.0));
            }
            None => {}
        }

        if let Some(err) = player.error() {
            ui.colored_label(
                super::theme::danger(ui.visuals()),
                format!("Playback failed: {err}"),
            );
        }

        ui.separator();

        const COLS: usize = 3;
        egui::ScrollArea::vertical()
            .max_height(360.0)
            .show(ui, |ui| {
                egui::Grid::new("optical_audio_tracks_grid")
                    .num_columns(COLS)
                    .spacing([18.0, 8.0])
                    .show(ui, |ui| {
                        for (i, t) in player.audio_tracks().enumerate() {
                            let this = now.filter(|(n, _)| *n == t.number);
                            ui.horizontal(|ui| {
                                if this.is_some() {
                                    if ui.button("Stop").on_hover_text("Stop").clicked() {
                                        actions.push(AudioAction::Stop);
                                    }
                                } else if ui.add_enabled(!busy, egui::Button::new("Play")).clicked()
                                {
                                    actions.push(AudioAction::Play(t.number));
                                }
                                match this {
                                    Some((_, PlayState::Playing(secs))) => {
                                        let pos = secs.min(f64::from(t.frames) / 75.0);
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
    // Escape or a click outside closes it too.
    if modal.should_close() {
        actions.push(AudioAction::Close);
    }
}
