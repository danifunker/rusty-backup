//! The CHD options control where the engine has no CHD support (the vintage build): the same
//! types and calls as `chd.rs`, adding no controls. No screen offers CHD there, so Start carries
//! the profile's defaults.

use rusty_backup::controller::backup::{ChdOptions, ChdProfile};

use super::form::Form;
use super::layout::Node;

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

/// The options a CHD screen starts from: only the profile's defaults here.
#[derive(Debug, Clone)]
pub struct ChdOptionsControl {
    pub custom: ChdOptions,
}

impl ChdOptionsControl {
    pub fn new(profile: ChdProfile) -> Self {
        ChdOptionsControl {
            custom: ChdOptions::defaults_for(profile),
        }
    }
}

pub struct ChdControl<K: Copy + PartialEq + 'static> {
    profile: ChdProfile,
    _wrap: fn(ChdKey) -> K,
}

impl<K: Copy + PartialEq + 'static> ChdControl<K> {
    pub fn build(
        _form: &Form<K>,
        wrap: fn(ChdKey) -> K,
        _ids: ChdIds,
        _state: ChdOptionsControl,
        profile: ChdProfile,
    ) -> ChdControl<K> {
        ChdControl {
            profile,
            _wrap: wrap,
        }
    }

    pub fn is_valid(&self) -> bool {
        true
    }

    /// What Start carries: the profile's defaults, never hand-tuned.
    pub fn choice(&self) -> (ChdOptions, bool) {
        (ChdOptions::defaults_for(self.profile), false)
    }

    pub fn set_visible(&self, _form: &Form<K>, _on: bool) {}

    pub fn rows(&self) -> Vec<Node<K>> {
        Vec::new()
    }

    pub fn handle(&self, _form: &Form<K>, _k: ChdKey) -> bool {
        false
    }
}
