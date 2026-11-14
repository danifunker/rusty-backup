//! The "Advanced CHD options" control: widget state a screen hands to its controller on Start.
//!
//! A part of its parent's form, not a window: the parent lays out [`ChdControl::rows`] and passes
//! the actions of the control's keys to [`ChdControl::handle`]. Backup, Optical, Export and Convert
//! reuse it; the Win32 shell's `chd.rs` is the reference.

use std::cell::RefCell;

use rusty_backup::controller::app::chd::{
    codec_choices, codec_long_name, hunk_presets, modes_for, unit_size_for, ChdOptions,
    ChdOptionsControl, ChdOptionsMode, ChdProfile,
};

use crate::form::Form;
use crate::layout::{self, Node, Size};

/// The control's own parts, which the parent's key wraps.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChdKey {
    Caption(usize),
    Mode,
    Hunk,
    Codec(usize),
    Error,
}

/// The widget ids a screen gives the control.
#[derive(Clone, Copy)]
pub struct ChdIds {
    pub mode: &'static str,
    pub hunk: &'static str,
    pub codec: &'static str,
}

pub struct ChdControl<K: Copy + PartialEq + 'static> {
    profile: ChdProfile,
    state: RefCell<ChdOptionsControl>,
    wrap: fn(ChdKey) -> K,
}

impl<K: Copy + PartialEq + 'static> ChdControl<K> {
    pub fn build(
        form: &Form<K>,
        wrap: fn(ChdKey) -> K,
        ids: ChdIds,
        state: ChdOptionsControl,
        profile: ChdProfile,
    ) -> ChdControl<K> {
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
        ];
        for (i, c) in captions.iter().enumerate() {
            form.label(wrap(ChdKey::Caption(i)), c);
        }
        form.popup(wrap(ChdKey::Mode), &modes, ids.mode);
        form.popup(wrap(ChdKey::Hunk), &[], ids.hunk);
        for n in 0..4 {
            form.popup(
                wrap(ChdKey::Codec(n)),
                &names,
                &format!("{}.{}", ids.codec, n + 1),
            );
        }
        form.label(wrap(ChdKey::Error), "");
        let c = ChdControl {
            profile,
            state: RefCell::new(state),
            wrap,
        };
        c.show(form);
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

    pub fn set_visible(&self, form: &Form<K>, on: bool) {
        let w = self.wrap;
        let custom = self.state.borrow().mode == ChdOptionsMode::Custom;
        for k in [ChdKey::Caption(0), ChdKey::Mode, ChdKey::Error] {
            form.show(w(k), on);
        }
        let mut detail = vec![ChdKey::Hunk];
        detail.extend((1..6).map(ChdKey::Caption));
        detail.extend((0..4).map(ChdKey::Codec));
        for k in detail {
            form.show(w(k), on && custom);
        }
    }

    /// The control's rows for the parent's layout; the parent prunes the hidden ones.
    pub fn rows(&self) -> Vec<Node<K>> {
        let w = self.wrap;
        let pair = |caption: ChdKey, field: ChdKey| {
            Node::row(vec![
                Node::leaf(w(caption), Size::Fixed(180), Size::Fixed(layout::ROW - 4)),
                Node::leaf(w(field), Size::Fixed(270), Size::Fixed(layout::ROW + 2)),
                Node::Space(Size::Stretch(1)),
            ])
        };
        let mut rows = vec![pair(ChdKey::Caption(0), ChdKey::Mode)];
        rows.push(pair(ChdKey::Caption(1), ChdKey::Hunk));
        for i in 0..4 {
            rows.push(pair(ChdKey::Caption(2 + i), ChdKey::Codec(i)));
        }
        rows.push(Node::leaf(
            w(ChdKey::Error),
            Size::Stretch(1),
            Size::Fixed(layout::ROW - 4),
        ));
        rows
    }

    /// One of the control's lists acted: take its pick. False when `k` is not the control's.
    pub fn handle(&self, form: &Form<K>, k: ChdKey) -> bool {
        let w = self.wrap;
        let Some(i) = form.picked(w(k)) else {
            return matches!(k, ChdKey::Mode | ChdKey::Hunk | ChdKey::Codec(_));
        };
        match k {
            ChdKey::Mode => {
                if let Some(m) = modes_for(self.profile).get(i) {
                    self.state.borrow_mut().mode = *m;
                }
            }
            ChdKey::Hunk => {
                if let Some(h) = hunk_presets(self.profile).get(i) {
                    self.state.borrow_mut().custom.hunk_size = *h;
                }
            }
            ChdKey::Codec(slot) => {
                if let Some(codec) = codec_choices(self.profile).get(i) {
                    self.state.borrow_mut().custom.codecs[slot] = *codec;
                }
            }
            ChdKey::Caption(_) | ChdKey::Error => return false,
        }
        self.show(form);
        true
    }

    fn show(&self, form: &Form<K>) {
        let w = self.wrap;
        let state = self.state.borrow().clone();
        let modes = modes_for(self.profile);
        form.select(
            w(ChdKey::Mode),
            modes.iter().position(|m| *m == state.mode).unwrap_or(0),
        );
        let presets = hunk_presets(self.profile);
        let current = state.custom.hunk_size;
        let mut labels: Vec<String> = presets.iter().map(u32::to_string).collect();
        if !presets.contains(&current) {
            labels.push(format!("{current} (custom)"));
        }
        form.set_items(w(ChdKey::Hunk), &labels);
        form.select(
            w(ChdKey::Hunk),
            presets
                .iter()
                .position(|p| *p == current)
                .unwrap_or(presets.len()),
        );
        let choices = codec_choices(self.profile);
        for (slot, codec) in state.custom.codecs.iter().enumerate().take(4) {
            if let Some(i) = choices.iter().position(|c| c == codec) {
                form.select(w(ChdKey::Codec(slot)), i);
            }
        }
        form.set_text(
            w(ChdKey::Error),
            &state.validate(self.profile).err().unwrap_or_default(),
        );
    }
}
