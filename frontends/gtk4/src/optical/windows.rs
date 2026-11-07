//! The tab's windows, each up while the controller says so: CHD Info, Add remote daemon, the player.

use std::cell::RefCell;

use adw::prelude::*;
use rusty_backup::controller::optical::audio::ids as audio_ids;
use rusty_backup::controller::optical::{ids, AudioAction, OpticalAction, OpticalController};

use super::ActionFn;
use crate::browser::dialogs::{page, text_view, window, Slot};
use crate::effects::EffectHost;
use crate::form::{button, line, set_line};
use crate::widgets::{plain, set_text};

/// What the Add remote daemon window shows.
pub struct RemoteShown {
    connecting: bool,
    error: Option<String>,
    recent: Vec<String>,
}

impl RemoteShown {
    pub fn read(ctrl: &OpticalController) -> Option<RemoteShown> {
        ctrl.add_remote_open().then(|| RemoteShown {
            connecting: ctrl.remote_connecting().is_some(),
            error: ctrl.add_remote_error().map(str::to_string),
            recent: ctrl.recent_daemons().to_vec(),
        })
    }
}

struct RemoteParts {
    entry: gtk::Entry,
    connect: gtk::Button,
    spinner: gtk::Spinner,
    connecting: gtk::Box,
    error: gtk::Label,
    recent: Vec<gtk::Button>,
}

/// One track of the player window.
#[derive(Clone, PartialEq)]
struct TrackShown {
    number: u32,
    idle: String,
    /// This track plays: its row while it does, which is empty while it prepares.
    now: Option<Option<String>>,
}

/// What the player window shows.
pub struct AudioShown {
    tracks: Vec<TrackShown>,
    heading: Option<(String, Option<f64>)>,
    error: Option<String>,
}

impl AudioShown {
    pub fn read(ctrl: &OpticalController) -> Option<AudioShown> {
        let player = ctrl.audio();
        if !player.window_open() {
            return None;
        }
        let now = player.readout();
        let tracks = player.audio_tracks().map(|t| TrackShown {
            number: t.number,
            idle: format!("Track {} ({})", t.number, t.duration_mmss()),
            now: now
                .as_ref()
                .filter(|n| n.track == t.number)
                .map(|n| n.row.clone()),
        });
        Some(AudioShown {
            tracks: tracks.collect(),
            heading: now.as_ref().map(|n| (n.heading.clone(), n.fraction)),
            error: player.error().map(|e| format!("Playback failed: {e}")),
        })
    }
}

struct TrackRow {
    action: gtk::Button,
    label: gtk::Label,
}

struct AudioParts {
    heading: gtk::Label,
    bar: gtk::ProgressBar,
    error: gtk::Label,
    rows: Vec<TrackRow>,
}

pub struct Windows {
    host: EffectHost,
    send: ActionFn,
    chd_info: Slot,
    remote: Slot,
    remote_parts: RefCell<Option<RemoteParts>>,
    audio: Slot,
    audio_parts: RefCell<Option<AudioParts>>,
}

impl Windows {
    pub fn new(host: EffectHost, send: ActionFn) -> Windows {
        Windows {
            host,
            send,
            chd_info: Slot::default(),
            remote: Slot::default(),
            remote_parts: RefCell::default(),
            audio: Slot::default(),
            audio_parts: RefCell::default(),
        }
    }

    pub fn show(
        &self,
        chd_info: Option<String>,
        remote: Option<RemoteShown>,
        audio: Option<AudioShown>,
    ) {
        self.show_chd_info(chd_info);
        self.show_remote(remote);
        self.show_audio(audio);
    }

    fn show_chd_info(&self, text: Option<String>) {
        let close = Some(OpticalAction::CloseChdInfo);
        let build = text.clone();
        self.chd_info.sync(&self.host, &self.send, text, close, || {
            let body = page();
            body.append(&text_view(build.as_deref().unwrap_or("")));
            window("CHD Info", 560, 420, &body)
        });
    }

    fn show_remote(&self, shown: Option<RemoteShown>) {
        let key = shown.as_ref().map(|s| s.recent.join("\n"));
        let close = Some(OpticalAction::CloseAddRemote);
        self.remote.sync(&self.host, &self.send, key, close, || {
            let recent = shown.as_ref().map_or(&[][..], |s| s.recent.as_slice());
            let (parts, dialog) = remote_window(recent, &self.send);
            *self.remote_parts.borrow_mut() = Some(parts);
            dialog
        });
        let parts = self.remote_parts.borrow();
        let (Some(s), Some(p)) = (&shown, parts.as_ref()) else {
            return;
        };
        p.entry.set_sensitive(!s.connecting);
        p.connect.set_sensitive(!s.connecting);
        p.connecting.set_visible(s.connecting);
        p.spinner.set_spinning(s.connecting);
        set_line(&p.error, s.error.as_deref());
        for b in &p.recent {
            b.set_sensitive(!s.connecting);
        }
    }

    fn show_audio(&self, shown: Option<AudioShown>) {
        // The disc's tracks are the window: where the track is goes into it, not into the key.
        let key = shown.as_ref().map(|s| {
            let numbers = s.tracks.iter().map(|t| t.idle.as_str());
            numbers.collect::<Vec<_>>().join("\n")
        });
        let close = Some(OpticalAction::Audio(AudioAction::Close));
        self.audio.sync(&self.host, &self.send, key, close, || {
            let tracks = shown.as_ref().map_or(&[][..], |s| s.tracks.as_slice());
            let (parts, dialog) = audio_window(tracks, &self.send);
            *self.audio_parts.borrow_mut() = Some(parts);
            dialog
        });
        let parts = self.audio_parts.borrow();
        let (Some(s), Some(p)) = (&shown, parts.as_ref()) else {
            return;
        };
        let heading = s.heading.as_ref().map(|h| h.0.as_str());
        set_line(&p.heading, heading);
        let fraction = s.heading.as_ref().and_then(|h| h.1);
        p.bar.set_visible(fraction.is_some());
        p.bar.set_fraction(fraction.unwrap_or(0.0));
        set_line(&p.error, s.error.as_deref());
        let busy = s.heading.is_some();
        for (row, t) in p.rows.iter().zip(&s.tracks) {
            let this = t.now.is_some();
            let label = if this { "Stop" } else { "Play" };
            if row.action.label().as_deref() != Some(label) {
                row.action.set_label(label);
            }
            row.action.set_sensitive(this || !busy);
            let text = t.now.clone().flatten().unwrap_or_else(|| t.idle.clone());
            set_text(&row.label, &text);
        }
    }
}

fn remote_window(recent: &[String], send: &ActionFn) -> (RemoteParts, adw::Dialog) {
    let body = page();
    body.append(
        &gtk::Label::builder()
            .label("Daemon address (host:port):")
            .xalign(0.0)
            .build(),
    );
    let entry = gtk::Entry::builder()
        .placeholder_text("mister.local:7341")
        .activates_default(false)
        .build();
    entry.set_widget_name(ids::REMOTE_ADDRESS);
    body.append(&entry);
    let connect = button("Connect", ids::REMOTE_CONNECT);
    connect.add_css_class("suggested-action");
    let (s, typed) = (send.clone(), entry.clone());
    connect.connect_clicked(move |_| s(OpticalAction::ConnectRemote(typed.text().to_string())));
    let s = send.clone();
    entry.connect_activate(move |e| s(OpticalAction::ConnectRemote(e.text().to_string())));
    let cancel = button("Cancel", ids::REMOTE_CANCEL);
    let s = send.clone();
    cancel.connect_clicked(move |_| s(OpticalAction::CloseAddRemote));
    let row = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    row.append(&connect);
    row.append(&cancel);
    body.append(&row);

    let spinner = gtk::Spinner::new();
    let connecting = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    connecting.append(&spinner);
    connecting.append(&gtk::Label::new(Some("Connecting...")));
    connecting.set_visible(false);
    body.append(&connecting);
    let error = line();
    error.add_css_class("error");
    body.append(&error);

    let mut buttons = Vec::new();
    if !recent.is_empty() {
        body.append(&gtk::Separator::new(gtk::Orientation::Horizontal));
        body.append(&gtk::Label::builder().label("Recent:").xalign(0.0).build());
    }
    for addr in recent {
        let b = gtk::Button::with_label(&plain(addr));
        b.add_css_class("flat");
        let (s, typed, addr) = (send.clone(), entry.clone(), addr.clone());
        b.connect_clicked(move |_| {
            typed.set_text(&plain(&addr));
            s(OpticalAction::ConnectRemote(addr.clone()));
        });
        body.append(&b);
        buttons.push(b);
    }
    let parts = RemoteParts {
        entry,
        connect,
        spinner,
        connecting,
        error,
        recent: buttons,
    };
    (parts, window("Add remote daemon", 380, 300, &body))
}

fn audio_window(tracks: &[TrackShown], send: &ActionFn) -> (AudioParts, adw::Dialog) {
    const COLUMNS: i32 = 3;
    let body = page();
    let heading = line();
    heading.add_css_class("heading");
    body.append(&heading);
    let bar = gtk::ProgressBar::new();
    bar.set_visible(false);
    body.append(&bar);
    let error = line();
    error.add_css_class("error");
    body.append(&error);
    body.append(&gtk::Separator::new(gtk::Orientation::Horizontal));

    let grid = gtk::Grid::builder()
        .column_spacing(18)
        .row_spacing(8)
        .build();
    let mut rows = Vec::new();
    for (i, t) in tracks.iter().enumerate() {
        let action = button("Play", &format!("{}.{}", audio_ids::PLAY, t.number));
        let number = t.number;
        let s = send.clone();
        action.connect_clicked(move |b| {
            let stop = b.label().as_deref() == Some("Stop");
            let audio = match stop {
                true => AudioAction::Stop,
                false => AudioAction::Play(number),
            };
            s(OpticalAction::Audio(audio));
        });
        let label = gtk::Label::new(Some(&plain(&t.idle)));
        let cell = gtk::Box::new(gtk::Orientation::Horizontal, 6);
        cell.append(&action);
        cell.append(&label);
        grid.attach(&cell, i as i32 % COLUMNS, i as i32 / COLUMNS, 1, 1);
        rows.push(TrackRow { action, label });
    }
    let scroll = gtk::ScrolledWindow::builder()
        .child(&grid)
        .vexpand(true)
        .min_content_height(120)
        .build();
    body.append(&scroll);
    let close = button("Close", audio_ids::CLOSE);
    close.set_halign(gtk::Align::End);
    let s = send.clone();
    close.connect_clicked(move |_| s(OpticalAction::Audio(AudioAction::Close)));
    body.append(&close);
    let parts = AudioParts {
        heading,
        bar,
        error,
        rows,
    };
    (parts, window("Audio Tracks", 520, 420, &body))
}
