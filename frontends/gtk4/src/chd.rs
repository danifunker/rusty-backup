//! The "Advanced CHD options" control: widget state the screen hands to its controller on Start.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::app::chd::{
    codec_choices, codec_label, codec_long_name, hunk_presets, modes_for, unit_size_for,
    ChdOptionsControl, ChdOptionsMode, ChdProfile,
};

use crate::form::{combo, group, line, set_combo, set_labels, set_line};
use crate::sync::SyncGuard;

/// The widget ids a screen gives the control.
#[derive(Clone, Copy)]
pub struct ChdIds {
    pub mode: &'static str,
    pub hunk: &'static str,
    pub codec: &'static str,
}

pub struct ChdControl {
    root: adw::PreferencesGroup,
    profile: ChdProfile,
    state: RefCell<ChdOptionsControl>,
    mode: adw::ComboRow,
    hunk: adw::ComboRow,
    codecs: [adw::ComboRow; 4],
    error: gtk::Label,
    sync: SyncGuard,
}

impl ChdControl {
    /// `on_change` runs after each edit, so the screen can gate Start on [`Self::is_valid`].
    pub fn build(
        title: &str,
        note: &str,
        ids: ChdIds,
        state: ChdOptionsControl,
        profile: ChdProfile,
        on_change: Rc<dyn Fn()>,
    ) -> Rc<ChdControl> {
        let root = group(title);
        root.set_description(Some(note));
        let modes = modes_for(profile);
        let labels: Vec<&str> = modes.iter().map(|m| m.label()).collect();
        let mode = combo("Advanced", ids.mode, &labels);
        let hunk = combo("Hunk size", ids.hunk, &[]);
        hunk.set_subtitle(&format!("Unit {} bytes", unit_size_for(profile)));
        let choices = codec_choices(profile);
        let names: Vec<String> = choices.iter().map(|c| codec_long_name(*c)).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let codecs = [1, 2, 3, 4].map(|n| {
            let id = format!("{}.{n}", ids.codec);
            combo(&format!("Codec {n}"), &id, &names)
        });
        let error = line();
        error.add_css_class("error");
        root.add(&mode);
        root.add(&hunk);
        for row in &codecs {
            root.add(row);
        }
        root.add(&error);

        let control = Rc::new(ChdControl {
            root,
            profile,
            state: RefCell::new(state),
            mode,
            hunk,
            codecs,
            error,
            sync: SyncGuard::default(),
        });
        let me = Rc::downgrade(&control);
        let changed = on_change.clone();
        control.mode.connect_selected_notify(move |row| {
            let Some(c) = me.upgrade().filter(|c| !c.sync.active()) else {
                return;
            };
            if let Some(mode) = modes.get(row.selected() as usize) {
                c.state.borrow_mut().mode = *mode;
            }
            c.show();
            changed();
        });
        let me = Rc::downgrade(&control);
        let changed = on_change.clone();
        control.hunk.connect_selected_notify(move |row| {
            let Some(c) = me.upgrade().filter(|c| !c.sync.active()) else {
                return;
            };
            if let Some(hunk) = hunk_presets(c.profile).get(row.selected() as usize) {
                c.state.borrow_mut().custom.hunk_size = *hunk;
            }
            c.show();
            changed();
        });
        for (slot, row) in control.codecs.iter().enumerate() {
            let me = Rc::downgrade(&control);
            let changed = on_change.clone();
            let choices = choices.clone();
            row.connect_selected_notify(move |row| {
                let Some(c) = me.upgrade().filter(|c| !c.sync.active()) else {
                    return;
                };
                if let Some(codec) = choices.get(row.selected() as usize) {
                    c.state.borrow_mut().custom.codecs[slot] = *codec;
                }
                changed();
            });
        }
        control.show();
        control
    }

    pub fn root(&self) -> &adw::PreferencesGroup {
        &self.root
    }

    pub fn is_valid(&self) -> bool {
        self.state.borrow().is_valid(self.profile)
    }

    /// What Start carries: the concrete options and whether they are hand-tuned.
    pub fn choice(&self) -> (rusty_backup::controller::app::chd::ChdOptions, bool) {
        let state = self.state.borrow();
        (
            state.effective(self.profile),
            state.mode == ChdOptionsMode::Custom,
        )
    }

    fn show(&self) {
        self.sync.run(|| {
            let state = self.state.borrow().clone();
            let modes = modes_for(self.profile);
            let index = modes.iter().position(|m| *m == state.mode).unwrap_or(0);
            set_combo(&self.mode, index as u32);

            let custom = state.mode == ChdOptionsMode::Custom;
            self.hunk.set_visible(custom);
            let presets = hunk_presets(self.profile);
            let current = state.custom.hunk_size;
            let mut labels: Vec<String> = presets.iter().map(u32::to_string).collect();
            if !presets.contains(&current) {
                labels.push(format!("{current} (custom)"));
            }
            set_labels(&self.hunk, &labels);
            let hunk = presets.iter().position(|p| *p == current);
            set_combo(&self.hunk, hunk.unwrap_or(presets.len()) as u32);

            let choices = codec_choices(self.profile);
            for (slot, row) in self.codecs.iter().enumerate() {
                row.set_visible(custom);
                let codec = state.custom.codecs[slot];
                row.set_tooltip_text(Some(codec_label(codec).as_str()));
                if let Some(i) = choices.iter().position(|c| *c == codec) {
                    set_combo(row, i as u32);
                }
            }
            let error = state.validate(self.profile).err();
            set_line(&self.error, error.as_deref());
        });
    }
}
