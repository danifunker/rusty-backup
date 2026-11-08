//! GTK key events as the controller's key map names them: every shortcut lookup goes through here.

use gtk::gdk;
use rusty_backup::controller::keys::{Key, Mods};

/// The key map's name for `key`; `None` for a key no binding can use (a bare modifier, a dead key).
pub fn key_of(key: gdk::Key) -> Option<Key> {
    use gdk::Key as G;
    let fixed = [
        (G::Tab, Key::Tab),
        (G::ISO_Left_Tab, Key::Tab),
        (G::KP_Tab, Key::Tab),
        (G::Return, Key::Enter),
        (G::KP_Enter, Key::Enter),
        (G::ISO_Enter, Key::Enter),
        (G::Escape, Key::Escape),
        (G::BackSpace, Key::Backspace),
        (G::Delete, Key::Delete),
        (G::KP_Delete, Key::Delete),
        (G::Insert, Key::Insert),
        (G::KP_Insert, Key::Insert),
        (G::space, Key::Space),
        (G::KP_Space, Key::Space),
        (G::Up, Key::Up),
        (G::KP_Up, Key::Up),
        (G::Down, Key::Down),
        (G::KP_Down, Key::Down),
        (G::Left, Key::Left),
        (G::KP_Left, Key::Left),
        (G::Right, Key::Right),
        (G::KP_Right, Key::Right),
        (G::Page_Up, Key::PageUp),
        (G::KP_Page_Up, Key::PageUp),
        (G::Page_Down, Key::PageDown),
        (G::KP_Page_Down, Key::PageDown),
        (G::Home, Key::Home),
        (G::KP_Home, Key::Home),
        (G::End, Key::End),
        (G::KP_End, Key::End),
        (G::F1, Key::F(1)),
        (G::F2, Key::F(2)),
        (G::F3, Key::F(3)),
        (G::F4, Key::F(4)),
        (G::F5, Key::F(5)),
        (G::F6, Key::F(6)),
        (G::F7, Key::F(7)),
        (G::F8, Key::F(8)),
        (G::F9, Key::F(9)),
        (G::F10, Key::F(10)),
        (G::F11, Key::F(11)),
        (G::F12, Key::F(12)),
    ];
    if let Some((_, k)) = fixed.iter().find(|(g, _)| *g == key) {
        return Some(*k);
    }
    let c = key.to_lower().to_unicode()?;
    (c.is_ascii_graphic()).then_some(Key::Char(c))
}

pub fn mods_of(state: gdk::ModifierType) -> Mods {
    use gdk::ModifierType as M;
    Mods {
        ctrl: state.contains(M::CONTROL_MASK),
        alt: state.contains(M::ALT_MASK),
        shift: state.contains(M::SHIFT_MASK),
        cmd: state.intersects(M::META_MASK | M::SUPER_MASK),
    }
}
