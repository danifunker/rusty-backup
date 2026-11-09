//! The "Advanced CHD options" control: widget state a screen hands to its controller on Start.
//!
//! A part of its parent window, not a window: the parent lays out [`ChdControl::rows`] and passes
//! its `WM_COMMAND`s through [`ChdControl::command`]. Backup, Optical and Convert reuse it.

use std::cell::RefCell;

use rusty_backup::controller::app::chd::{
    codec_choices, codec_long_name, hunk_presets, modes_for, unit_size_for, ChdOptions,
    ChdOptionsControl, ChdOptionsMode, ChdProfile,
};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, WPARAM};
use windows::Win32::UI::WindowsAndMessaging::{
    SendMessageW, CBN_SELCHANGE, CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL, CB_RESETCONTENT,
    CB_SETCURSEL, WINDOW_EX_STYLE, WINDOW_STYLE, WS_TABSTOP, WS_VSCROLL,
};

use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::ui;

/// The widget ids a screen gives the control.
#[derive(Clone, Copy)]
pub struct ChdIds {
    pub mode: &'static str,
    pub hunk: &'static str,
    pub codec: &'static str,
}

pub struct ChdControl {
    profile: ChdProfile,
    state: RefCell<ChdOptionsControl>,
    captions: [HWND; 6],
    mode: HWND,
    hunk: HWND,
    codecs: [HWND; 4],
    error: HWND,
    sync: SyncGuard,
}

fn combo(parent: HWND, id: &str, items: &[String]) -> HWND {
    let style = WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
    let h = ui::child(parent, w!("COMBOBOX"), "", style, WINDOW_EX_STYLE(0), id);
    set_items(h, items);
    h
}

fn set_items(h: HWND, items: &[String]) {
    // SAFETY: plain combo box messages; the control copies each string.
    unsafe {
        SendMessageW(h, CB_RESETCONTENT, None, None);
        for item in items {
            let t = ui::wide(item);
            SendMessageW(h, CB_ADDSTRING, None, Some(LPARAM(t.as_ptr() as isize)));
        }
    }
}

fn selected(h: HWND) -> Option<usize> {
    // SAFETY: a plain combo box query.
    usize::try_from(unsafe { SendMessageW(h, CB_GETCURSEL, None, None).0 }).ok()
}

fn select(h: HWND, i: usize) {
    if selected(h) != Some(i) {
        // SAFETY: a plain combo box message; it sends no CBN_SELCHANGE.
        unsafe {
            SendMessageW(h, CB_SETCURSEL, Some(WPARAM(i)), None);
        }
    }
}

impl ChdControl {
    pub fn build(
        parent: HWND,
        ids: ChdIds,
        state: ChdOptionsControl,
        profile: ChdProfile,
    ) -> ChdControl {
        let modes: Vec<String> = modes_for(profile)
            .iter()
            .map(|m| m.label().to_string())
            .collect();
        let names: Vec<String> = codec_choices(profile)
            .iter()
            .map(|c| codec_long_name(*c))
            .collect();
        let captions = [
            "CHD options:".to_string(),
            format!("Hunk size (unit {} bytes):", unit_size_for(profile)),
            "Codec 1:".into(),
            "Codec 2:".into(),
            "Codec 3:".into(),
            "Codec 4:".into(),
        ]
        .map(|t| ui::label(parent, &t));
        let mode = combo(parent, ids.mode, &modes);
        let hunk = combo(parent, ids.hunk, &[]);
        let codecs = [1, 2, 3, 4].map(|n| combo(parent, &format!("{}.{n}", ids.codec), &names));
        let error = ui::label(parent, "");
        let c = ChdControl {
            profile,
            state: RefCell::new(state),
            captions,
            mode,
            hunk,
            codecs,
            error,
            sync: SyncGuard::default(),
        };
        c.show();
        c
    }

    pub fn is_valid(&self) -> bool {
        self.state.borrow().is_valid(self.profile)
    }

    /// What Start carries: the concrete options and whether they are hand-tuned.
    pub fn choice(&self) -> (ChdOptions, bool) {
        let state = self.state.borrow();
        (
            state.effective(self.profile),
            state.mode == ChdOptionsMode::Custom,
        )
    }

    pub fn set_visible(&self, on: bool) {
        let custom = self.state.borrow().mode == ChdOptionsMode::Custom;
        for h in [self.captions[0], self.mode, self.error] {
            ui::set_visible(h, on);
        }
        for h in self.captions[1..]
            .iter()
            .chain([&self.hunk])
            .chain(self.codecs.iter())
        {
            ui::set_visible(*h, on && custom);
        }
    }

    /// The control's rows for the parent's layout: only the visible ones.
    pub fn rows(&self) -> Vec<Node<HWND>> {
        let pair = |caption: HWND, field: HWND| {
            Node::row(vec![
                Node::leaf(caption, Size::Fixed(170), Size::Fixed(layout::ROW - 8)),
                Node::leaf(field, Size::Fixed(260), Size::Fixed(layout::ROW)),
            ])
        };
        let mut rows = Vec::new();
        if !ui::shown(self.mode) {
            return rows;
        }
        rows.push(pair(self.captions[0], self.mode));
        if ui::shown(self.hunk) {
            rows.push(pair(self.captions[1], self.hunk));
            for (i, c) in self.codecs.iter().enumerate() {
                rows.push(pair(self.captions[2 + i], *c));
            }
        }
        rows.push(Node::leaf(
            self.error,
            Size::Stretch(1),
            Size::Fixed(layout::ROW - 8),
        ));
        rows
    }

    /// The control's drop-down lists, whose windows are taller than their row.
    pub fn combos(&self) -> Vec<HWND> {
        let mut v = vec![self.mode, self.hunk];
        v.extend(self.codecs);
        v
    }

    /// Handle a `WM_COMMAND` from one of the control's lists; true when it was one of them.
    pub fn command(&self, from: HWND, code: u32) -> bool {
        if code != CBN_SELCHANGE || self.sync.active() {
            return self.combos().contains(&from);
        }
        let Some(i) = selected(from) else {
            return false;
        };
        if from == self.mode {
            if let Some(m) = modes_for(self.profile).get(i) {
                self.state.borrow_mut().mode = *m;
            }
        } else if from == self.hunk {
            if let Some(h) = hunk_presets(self.profile).get(i) {
                self.state.borrow_mut().custom.hunk_size = *h;
            }
        } else if let Some(slot) = self.codecs.iter().position(|c| *c == from) {
            if let Some(codec) = codec_choices(self.profile).get(i) {
                self.state.borrow_mut().custom.codecs[slot] = *codec;
            }
        } else {
            return false;
        }
        self.show();
        true
    }

    fn show(&self) {
        self.sync.run(|| {
            let state = self.state.borrow().clone();
            let modes = modes_for(self.profile);
            select(
                self.mode,
                modes.iter().position(|m| *m == state.mode).unwrap_or(0),
            );
            let presets = hunk_presets(self.profile);
            let current = state.custom.hunk_size;
            let mut labels: Vec<String> = presets.iter().map(u32::to_string).collect();
            if !presets.contains(&current) {
                labels.push(format!("{current} (custom)"));
            }
            set_items(self.hunk, &labels);
            select(
                self.hunk,
                presets
                    .iter()
                    .position(|p| *p == current)
                    .unwrap_or(presets.len()),
            );
            let choices = codec_choices(self.profile);
            for (slot, h) in self.codecs.iter().enumerate() {
                if let Some(i) = choices.iter().position(|c| *c == state.custom.codecs[slot]) {
                    select(*h, i);
                }
            }
            ui::set_text(
                self.error,
                &state.validate(self.profile).err().unwrap_or_default(),
            );
        });
    }
}
