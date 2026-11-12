//! The menu bar (agreed draft: docs/mockups/appkit-draft.html). The shell's own items carry a
//! controller id, go to the action target and follow the state of the control with the same id;
//! the standard ones go to the first responder. Shortcuts come from the controller's table.

use std::cell::RefCell;

use objc2::rc::Retained;
use objc2::runtime::Sel;
use objc2::{sel, MainThreadMarker};
use objc2_app_kit::{
    NSApplication, NSEventModifierFlags, NSMenu, NSMenuItem, NSUserInterfaceItemIdentification,
};
use rusty_backup::controller::app::{about, ids, AppTab, MENU_SHORTCUTS};
use rusty_backup::controller::inspect::ids as inspect_ids;
use rusty_backup::controller::keys::{Chord, Key};

use crate::ui::{ns, target};

/// The Help menu's items that open the project's pages.
pub const WEBSITE: &str = "app.website";
pub const ISSUES: &str = "app.issues";

/// Items that are always on: they switch to their screen first.
const ALWAYS: [&str; 7] = [
    ids::ABOUT,
    ids::UPDATE_CHECK,
    WEBSITE,
    ISSUES,
    inspect_ids::OPEN_FILE,
    inspect_ids::OPEN_BACKUP,
    ids::LOG_COPY,
];

thread_local! {
    static ITEMS: RefCell<Vec<(String, Retained<NSMenuItem>)>> = const { RefCell::new(Vec::new()) };
    static SOURCES: RefCell<Option<(Retained<NSMenu>, Retained<NSMenu>)>> = const { RefCell::new(None) };
}

fn item(mtm: MainThreadMarker, title: &str, action: Sel, key: &str) -> Retained<NSMenuItem> {
    // SAFETY: a plain menu item; its action goes up the responder chain.
    unsafe {
        NSMenuItem::initWithTitle_action_keyEquivalent(
            mtm.alloc(),
            &ns(title),
            Some(action),
            &ns(key),
        )
    }
}

/// The key equivalent and modifiers for a chord the controller defines.
fn equivalent(chord: &str) -> Option<(String, NSEventModifierFlags)> {
    let c = Chord::parse(chord).ok()?;
    let key = match c.key {
        Key::Char(ch) => ch.to_ascii_lowercase().to_string(),
        _ => return None,
    };
    let mut mods = NSEventModifierFlags::empty();
    if c.primary {
        mods |= NSEventModifierFlags::Command;
    }
    if c.alt {
        mods |= NSEventModifierFlags::Option;
    }
    if c.shift {
        mods |= NSEventModifierFlags::Shift;
    }
    Some((key, mods))
}

/// An item that goes to the action target with `id`, its shortcut from `chord`.
fn ours_with(
    mtm: MainThreadMarker,
    title: &str,
    id: &str,
    chord: Option<&str>,
) -> Retained<NSMenuItem> {
    let it = item(mtm, title, sel!(act:), "");
    if let Some((key, mods)) = chord.and_then(equivalent) {
        it.setKeyEquivalent(&ns(&key));
        it.setKeyEquivalentModifierMask(mods);
    }
    let t = target(mtm);
    // SAFETY: the target is never dropped.
    unsafe { it.setTarget(Some(&t)) };
    it.setIdentifier(Some(&ns(id)));
    ITEMS.with(|i| i.borrow_mut().push((id.to_string(), it.clone())));
    it
}

/// An item with the controller's shortcut for `id`, if it has one.
fn ours(mtm: MainThreadMarker, title: &str, id: &str) -> Retained<NSMenuItem> {
    let chord = MENU_SHORTCUTS
        .iter()
        .find(|(i, _)| *i == id)
        .map(|(_, c)| *c);
    ours_with(mtm, title, id, chord)
}

fn menu(
    mtm: MainThreadMarker,
    title: &str,
    items: Vec<Retained<NSMenuItem>>,
    auto: bool,
) -> Retained<NSMenu> {
    let m = NSMenu::initWithTitle(mtm.alloc(), &ns(title));
    m.setAutoenablesItems(auto);
    for it in items {
        m.addItem(&it);
    }
    m
}

fn submenu(mtm: MainThreadMarker, bar: &NSMenu, m: &NSMenu) {
    let top = NSMenuItem::new(mtm);
    top.setSubmenu(Some(m));
    bar.addItem(&top);
}

/// A submenu item holding `m`.
fn holder(mtm: MainThreadMarker, title: &str, m: &NSMenu) -> Retained<NSMenuItem> {
    let it = NSMenuItem::new(mtm);
    it.setTitle(&ns(title));
    it.setSubmenu(Some(m));
    it
}

pub fn build(mtm: MainThreadMarker) {
    let app = NSApplication::sharedApplication(mtm);
    let bar = NSMenu::new(mtm);
    let name = about::NAME;
    let sep = || NSMenuItem::separatorItem(mtm);
    let hide_others = item(mtm, "Hide Others", sel!(hideOtherApplications:), "h");
    hide_others
        .setKeyEquivalentModifierMask(NSEventModifierFlags::Command | NSEventModifierFlags::Option);
    let app_menu = menu(
        mtm,
        name,
        vec![
            ours(mtm, &format!("About {name}"), ids::ABOUT),
            ours(mtm, "Check for Updates...", ids::UPDATE_CHECK),
            sep(),
            ours(mtm, "Settings...", ids::SETTINGS),
            sep(),
            item(mtm, &format!("Hide {name}"), sel!(hide:), "h"),
            hide_others,
            item(mtm, "Show All", sel!(unhideAllApplications:), ""),
            sep(),
            item(mtm, &format!("Quit {name}"), sel!(terminate:), "q"),
        ],
        false,
    );
    submenu(mtm, &bar, &app_menu);

    let recent = menu(mtm, "Open Recent", Vec::new(), false);
    let devices = menu(mtm, "Open Device", Vec::new(), false);
    let file = menu(
        mtm,
        "File",
        vec![
            ours(mtm, "Open File for Inspect...", inspect_ids::OPEN_FILE),
            ours(mtm, "Open CD-ROM Image...", "optical.browse_image"),
            ours(mtm, "Open Backup Folder...", inspect_ids::OPEN_BACKUP),
            holder(mtm, "Open Recent", &recent),
            holder(mtm, "Open Device", &devices),
            ours(mtm, "Connect to Remote...", inspect_ids::CONNECT_REMOTE),
            sep(),
            ours(mtm, "Refresh Devices", ids::REFRESH_DEVICES),
            sep(),
            ours(mtm, "Close Backup", ids::CLOSE_BACKUP),
            ours(mtm, "Close", inspect_ids::CLOSE),
        ],
        false,
    );
    submenu(mtm, &bar, &file);
    SOURCES.with(|s| *s.borrow_mut() = Some((recent, devices)));

    let redo = item(mtm, "Redo", sel!(redo:), "z");
    redo.setKeyEquivalentModifierMask(NSEventModifierFlags::Command | NSEventModifierFlags::Shift);
    let edit = menu(
        mtm,
        "Edit",
        vec![
            item(mtm, "Undo", sel!(undo:), "z"),
            redo,
            sep(),
            item(mtm, "Cut", sel!(cut:), "x"),
            item(mtm, "Copy", sel!(copy:), "c"),
            item(mtm, "Paste", sel!(paste:), "v"),
            item(mtm, "Select All", sel!(selectAll:), "a"),
        ],
        true,
    );
    submenu(mtm, &bar, &edit);

    let mut screens: Vec<Retained<NSMenuItem>> = AppTab::ALL
        .iter()
        .map(|t| {
            ours_with(
                mtm,
                t.label(),
                &crate::toolbar::screen_id(*t),
                Some(t.shortcut()),
            )
        })
        .collect();
    screens.push(ours(mtm, "Commander", ids::COMMANDER));
    screens.push(sep());
    screens.push(ours(mtm, "Copy Log", ids::LOG_COPY));
    screens.push(ours(mtm, "Clear Log", ids::LOG_CLEAR));
    screens.push(ours(mtm, "Auto-scroll Log", ids::LOG_AUTO_SCROLL));
    submenu(mtm, &bar, &menu(mtm, "View", screens, false));

    let disk = menu(
        mtm,
        "Disk",
        vec![
            ours(mtm, "Re-inspect", inspect_ids::REINSPECT),
            sep(),
            ours(mtm, "Edit Partition Table...", inspect_ids::EDIT_TABLE),
            ours(mtm, "Add Partition...", inspect_ids::ADD_PARTITION),
            ours(mtm, "Resize Partitions...", inspect_ids::RESIZE),
            ours(mtm, "Expand Image...", inspect_ids::EXPAND_IMAGE),
            ours(mtm, "Make Bootable...", inspect_ids::MAKE_BOOTABLE),
            sep(),
            ours(mtm, "Export Disk Image...", inspect_ids::EXPORT),
            ours(mtm, "Convert...", inspect_ids::CONVERT),
            ours(mtm, "CHD Info", inspect_ids::CHD_INFO),
        ],
        false,
    );
    submenu(mtm, &bar, &disk);
    let partition = menu(
        mtm,
        "Partition",
        vec![
            ours(mtm, "Browse", inspect_ids::ROW_BROWSE),
            ours(mtm, "Calc min", inspect_ids::ROW_CALC_MIN),
            ours(mtm, "Check", inspect_ids::ROW_CHECK),
            ours(mtm, "Expand/Export...", inspect_ids::ROW_EXPAND_HFS),
            ours(mtm, "Defragment...", inspect_ids::ROW_DEFRAGMENT),
            ours(mtm, "Resize...", inspect_ids::ROW_RESIZE_ALTO),
        ],
        false,
    );
    submenu(mtm, &bar, &partition);

    let window = menu(
        mtm,
        "Window",
        vec![
            item(mtm, "Minimize", sel!(performMiniaturize:), "m"),
            item(mtm, "Zoom", sel!(performZoom:), ""),
        ],
        true,
    );
    submenu(mtm, &bar, &window);
    let help = menu(
        mtm,
        "Help",
        vec![
            ours(mtm, &format!("{name} Website"), WEBSITE),
            ours(mtm, "Report an Issue", ISSUES),
        ],
        false,
    );
    submenu(mtm, &bar, &help);
    app.setMainMenu(Some(&bar));
    app.setWindowsMenu(Some(&window));
}

/// Rebuild Open Recent and Open Device from the Inspect source menu's lists.
pub fn set_sources(recent: &[String], devices: &[String]) {
    let Some(mtm) = MainThreadMarker::new() else {
        return;
    };
    SOURCES.with(|s| {
        let Some((r, d)) = &*s.borrow() else { return };
        r.removeAllItems();
        for (i, path) in recent.iter().enumerate() {
            r.addItem(&ours_with(
                mtm,
                path,
                &format!("{}{i}", crate::inspect::RECENT_ITEM),
                None,
            ));
        }
        d.removeAllItems();
        for (i, name) in devices.iter().enumerate() {
            d.addItem(&ours_with(
                mtm,
                name,
                &format!("{}{i}", crate::inspect::DEVICE_ITEM),
                None,
            ));
        }
    });
    // The rebuilt lists' old items are gone; forget them.
    ITEMS.with(|i| {
        i.borrow_mut()
            // SAFETY: a plain query of the item's menu.
            .retain(|(_, it)| unsafe { it.menu() }.is_some());
    });
}

/// Each item follows its control: on when the control with its id is on screen and enabled.
pub fn sync(on: &dyn Fn(&str) -> Option<bool>) {
    ITEMS.with(|items| {
        for (id, it) in items.borrow().iter() {
            let want = if ALWAYS.contains(&id.as_str())
                || id.starts_with(crate::inspect::RECENT_ITEM)
                || id.starts_with(crate::inspect::DEVICE_ITEM)
            {
                true
            } else {
                on(id).unwrap_or_else(|| crate::ui::usable(id))
            };
            if it.isEnabled() != want {
                it.setEnabled(want);
            }
        }
    });
}
