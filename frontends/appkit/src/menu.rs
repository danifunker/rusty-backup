//! The menu bar: the app menu, Edit's standard items (so text fields copy and paste), Window and
//! Help. The shell's own items carry a controller id and go to the action target; the standard
//! ones go to the first responder.

use objc2::rc::Retained;
use objc2::runtime::Sel;
use objc2::{sel, MainThreadMarker};
use objc2_app_kit::{
    NSApplication, NSEventModifierFlags, NSMenu, NSMenuItem, NSUserInterfaceItemIdentification,
};
use rusty_backup::controller::app::{about, ids};

use crate::ui::{ns, target};

/// The Help menu's item that opens the project page.
pub const WEBSITE: &str = "app.website";

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

/// An item that goes to the action target with `id`.
fn ours(mtm: MainThreadMarker, title: &str, id: &str, key: &str) -> Retained<NSMenuItem> {
    let it = item(mtm, title, objc2::sel!(act:), key);
    let t = target(mtm);
    // SAFETY: the target is never dropped.
    unsafe { it.setTarget(Some(&t)) };
    it.setIdentifier(Some(&ns(id)));
    it
}

fn submenu(
    mtm: MainThreadMarker,
    bar: &NSMenu,
    title: &str,
    items: Vec<Retained<NSMenuItem>>,
) -> Retained<NSMenu> {
    let menu = NSMenu::initWithTitle(mtm.alloc(), &ns(title));
    for it in items {
        menu.addItem(&it);
    }
    let top = NSMenuItem::new(mtm);
    top.setSubmenu(Some(&menu));
    bar.addItem(&top);
    menu
}

pub fn build(mtm: MainThreadMarker) {
    let app = NSApplication::sharedApplication(mtm);
    let bar = NSMenu::new(mtm);
    let name = about::NAME;
    let hide_others = item(mtm, "Hide Others", sel!(hideOtherApplications:), "h");
    hide_others
        .setKeyEquivalentModifierMask(NSEventModifierFlags::Command | NSEventModifierFlags::Option);
    // Settings arrives in slice 7b; until then its item shows but is off.
    let settings = ours(mtm, "Settings...", ids::SETTINGS, ",");
    settings.setEnabled(false);
    submenu(
        mtm,
        &bar,
        name,
        vec![
            ours(mtm, &format!("About {name}"), ids::ABOUT, ""),
            NSMenuItem::separatorItem(mtm),
            settings,
            NSMenuItem::separatorItem(mtm),
            item(mtm, &format!("Hide {name}"), sel!(hide:), "h"),
            hide_others,
            item(mtm, "Show All", sel!(unhideAllApplications:), ""),
            NSMenuItem::separatorItem(mtm),
            item(mtm, &format!("Quit {name}"), sel!(terminate:), "q"),
        ],
    );
    let redo = item(mtm, "Redo", sel!(redo:), "z");
    redo.setKeyEquivalentModifierMask(NSEventModifierFlags::Command | NSEventModifierFlags::Shift);
    submenu(
        mtm,
        &bar,
        "Edit",
        vec![
            item(mtm, "Undo", sel!(undo:), "z"),
            redo,
            NSMenuItem::separatorItem(mtm),
            item(mtm, "Cut", sel!(cut:), "x"),
            item(mtm, "Copy", sel!(copy:), "c"),
            item(mtm, "Paste", sel!(paste:), "v"),
            item(mtm, "Select All", sel!(selectAll:), "a"),
        ],
    );
    let window = submenu(
        mtm,
        &bar,
        "Window",
        vec![
            item(mtm, "Minimize", sel!(performMiniaturize:), "m"),
            item(mtm, "Zoom", sel!(performZoom:), ""),
        ],
    );
    submenu(
        mtm,
        &bar,
        "Help",
        vec![ours(mtm, &format!("{name} Website"), WEBSITE, "")],
    );
    app.setMainMenu(Some(&bar));
    app.setWindowsMenu(Some(&window));
}
