//! What a Mac shell's toolkit does for the screens: the controls of a [`Form`](super::form::Form).
//!
//! The AppKit shell implements it with objc2; the vintage shell over a C ABI in Objective-C. A
//! surface is a view controls sit on (a window's content, a page, a tab); a control is a small
//! integer the form hands out. Every call is on the main thread, and no call may run a surface's
//! handler while the host holds a borrow the handler's calls back in would need.

use std::rc::Rc;

use rusty_backup::controller::commander::Tone;

use super::layout::Rect;

/// A view controls sit on, numbered by the host.
pub type Surface = u32;

/// A control on a surface, numbered by its form.
pub type Ctl = u32;

/// What a control reports to its surface's handler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ev {
    /// Pressed, ticked, picked, or a pull-down item chosen.
    Act,
    /// The user typed in a field.
    Edited,
    /// A field's value was set from outside, through accessibility.
    ValueSet,
}

/// One pull-down item: its label, the control it acts as (none for a caption), and enabled.
pub type PullItem = (String, Option<Ctl>, bool);

pub trait Host {
    /// Send every report from surface `s` to `on`, replacing any handler before.
    fn bind(&self, s: Surface, on: Rc<dyn Fn(Ctl, Ev)>);
    /// Drop surface `s`'s controls and handler; the surface itself is the caller's.
    fn release(&self, s: Surface);

    fn button(&self, s: Surface, c: Ctl, title: &str, id: &str);
    fn check(&self, s: Surface, c: Ctl, title: &str, id: &str);
    /// A radio button; only radios of one `group` exclude each other.
    fn radio(&self, s: Surface, c: Ctl, group: usize, title: &str, id: &str);
    fn label(&self, s: Surface, c: Ctl, text: &str);
    /// A label that wraps onto as many lines as its frame gives it.
    fn wrap(&self, s: Surface, c: Ctl, text: &str);
    fn popup(&self, s: Surface, c: Ctl, items: &[String], id: &str);
    /// A pull-down whose first item is its title.
    fn pulldown(&self, s: Surface, c: Ctl, id: &str);
    fn set_pulldown(&self, s: Surface, c: Ctl, title: &str, items: &[PullItem]);
    /// An editable field; `caption` is its accessibility label.
    fn field(&self, s: Surface, c: Ctl, id: &str, caption: &str, secret: bool);
    fn progress(&self, s: Surface, c: Ctl);

    fn remove(&self, s: Surface, c: Ctl);
    fn shown(&self, s: Surface, c: Ctl) -> bool;
    fn show(&self, s: Surface, c: Ctl, on: bool);
    fn enable(&self, s: Surface, c: Ctl, on: bool);
    fn checked(&self, s: Surface, c: Ctl) -> bool;
    fn set_check(&self, s: Surface, c: Ctl, on: bool);
    fn set_title(&self, s: Surface, c: Ctl, title: &str);
    /// A label's or field's text, set as is; a field's set is reported as no edit.
    fn set_text(&self, s: Surface, c: Ctl, text: &str);
    fn set_tone(&self, s: Surface, c: Ctl, tone: Option<Tone>);
    /// The text now shown; reading it reports nothing.
    fn text(&self, s: Surface, c: Ctl) -> String;
    /// The field has the keyboard.
    fn typing(&self, s: Surface, c: Ctl) -> bool;
    fn picked(&self, s: Surface, c: Ctl) -> Option<usize>;
    fn select(&self, s: Surface, c: Ctl, i: usize);
    fn items(&self, s: Surface, c: Ctl) -> Vec<String>;
    fn set_items(&self, s: Surface, c: Ctl, items: &[String]);
    fn move_to(&self, s: Surface, c: Ctl, r: Rect);
}
