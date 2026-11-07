//! A partition's size mode and size, the cells Restore's table and the VHD window share.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::restore::{format_size, SizeMode};

use crate::widgets::set_text;

const MIB: u64 = 1024 * 1024;

/// The minimum still needs a walk, or one runs.
pub enum Deferred {
    /// Offers "Calc min", with this hover.
    Available(String),
    Pending(String),
}

/// What a size cell shows.
pub struct SizeShown {
    pub choices: Vec<SizeMode>,
    pub mode: SizeMode,
    /// The Custom size, as the controller will write it.
    pub custom_mib: u32,
    pub bounds: (u32, u32),
    /// The size this row restores to; ignored for Custom and Fill.
    pub effective: u64,
    pub deferred: Option<Deferred>,
}

pub struct SizeCell {
    mode: gtk::DropDown,
    spin: gtk::SpinButton,
    fixed: gtk::Label,
    calc: gtk::Button,
    spinner: gtk::Spinner,
    choices: Rc<RefCell<Vec<SizeMode>>>,
}

impl SizeCell {
    /// `on_change` gets the mode and Custom size; `on_calc` asks for the minimum.
    pub fn new(
        ids: (&str, &str),
        on_change: Rc<dyn Fn(SizeMode, u32)>,
        on_calc: Rc<dyn Fn()>,
    ) -> SizeCell {
        let mode = gtk::DropDown::from_strings(&[]);
        mode.set_widget_name(ids.0);
        let spin = gtk::SpinButton::with_range(1.0, 1.0, 1.0);
        spin.set_widget_name(ids.1);
        let fixed = gtk::Label::builder().xalign(1.0).build();
        let calc = gtk::Button::with_label("Calc min");
        calc.add_css_class("flat");
        calc.connect_clicked(move |_| on_calc());
        let spinner = gtk::Spinner::new();
        let choices: Rc<RefCell<Vec<SizeMode>>> = Rc::default();

        let held = choices.clone();
        let change = on_change.clone();
        let size = spin.clone();
        mode.connect_selected_notify(move |d| {
            let picked = held.borrow().get(d.selected() as usize).copied();
            if let Some(mode) = picked {
                change(mode, size.value() as u32);
            }
        });
        spin.connect_value_changed(move |s| on_change(SizeMode::Custom, s.value() as u32));
        SizeCell {
            mode,
            spin,
            fixed,
            calc,
            spinner,
            choices,
        }
    }

    /// Attach the mode and size cells at `column` and the one after it.
    pub fn attach(&self, grid: &gtk::Grid, column: i32, row: i32) {
        let mode = gtk::Box::new(gtk::Orientation::Horizontal, 4);
        mode.append(&self.mode);
        mode.append(&self.calc);
        mode.append(&self.spinner);
        let size = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        size.set_halign(gtk::Align::End);
        size.append(&self.spin);
        size.append(&self.fixed);
        grid.attach(&mode, column, row, 1, 1);
        grid.attach(&size, column + 1, row, 1, 1);
    }

    /// A hover on the size, such as the largest size a capped volume can take.
    pub fn set_tooltip(&self, text: Option<&str>) {
        self.spin.set_tooltip_text(text);
        self.mode.set_tooltip_text(text);
    }

    /// Run inside the screen's `SyncGuard`.
    pub fn show(&self, s: SizeShown) {
        // First: replacing the list fires selected-notify, which reads the choices.
        let changed = *self.choices.borrow() != s.choices;
        if changed {
            let labels: Vec<&str> = s.choices.iter().map(|m| m.label()).collect();
            *self.choices.borrow_mut() = s.choices.clone();
            self.mode.set_model(Some(&gtk::StringList::new(&labels)));
        }
        let index = s.choices.iter().position(|m| *m == s.mode).unwrap_or(0) as u32;
        if self.mode.selected() != index {
            self.mode.set_selected(index);
        }

        let custom = s.mode == SizeMode::Custom;
        self.spin.set_visible(custom);
        self.fixed.set_visible(!custom);
        let (lo, hi) = (f64::from(s.bounds.0), f64::from(s.bounds.1));
        let adjustment = self.spin.adjustment();
        if adjustment.lower() != lo || adjustment.upper() != hi {
            self.spin.set_range(lo, hi);
        }
        if custom && self.spin.value() != f64::from(s.custom_mib) {
            self.spin.set_value(f64::from(s.custom_mib));
        }
        if s.mode == SizeMode::FillRemaining {
            set_text(&self.fixed, "(auto)");
        } else {
            set_text(&self.fixed, &(s.effective / MIB).to_string());
        }
        self.fixed.set_tooltip_text(Some(&format_size(s.effective)));

        self.calc
            .set_visible(matches!(s.deferred, Some(Deferred::Available(_))));
        self.spinner
            .set_visible(matches!(s.deferred, Some(Deferred::Pending(_))));
        self.spinner
            .set_spinning(matches!(s.deferred, Some(Deferred::Pending(_))));
        match &s.deferred {
            Some(Deferred::Available(hint)) => self.calc.set_tooltip_text(Some(hint)),
            Some(Deferred::Pending(phase)) => self.spinner.set_tooltip_text(Some(phase)),
            None => {}
        }
    }
}
