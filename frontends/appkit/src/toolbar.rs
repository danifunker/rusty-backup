//! The screen switcher (D9): an `NSToolbar` of selectable items, one per screen, Commander at the
//! right end; the icons are the shared toolbar set (`assets/icons/toolbar/png`) as template images.

use objc2::rc::Retained;
use objc2::runtime::{NSObject, NSObjectProtocol, ProtocolObject};
use objc2::{define_class, msg_send, sel, AllocAnyThread, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSImage, NSToolbar, NSToolbarDelegate, NSToolbarDisplayMode,
    NSToolbarFlexibleSpaceItemIdentifier, NSToolbarItem,
};
use objc2_foundation::{NSArray, NSCopying, NSData, NSSize, NSString};
use rusty_backup::controller::app::{ids, AppTab};

use crate::ui::{ns, target};

/// The toolbar item id of a screen; `app::command` maps it back.
pub fn screen_id(tab: AppTab) -> String {
    format!("app.screen.{}", tab.icon())
}

pub fn tab_of(id: &str) -> Option<AppTab> {
    AppTab::ALL.iter().copied().find(|t| screen_id(*t) == id)
}

/// The 48-pixel icon, shown at 24 points so it is sharp on a Retina screen.
fn icon_png(name: &str) -> Option<&'static [u8]> {
    macro_rules! png {
        ($n:literal) => {
            include_bytes!(concat!("../../../assets/icons/toolbar/png/", $n, "-48.png"))
        };
    }
    Some(match name {
        "backup" => png!("backup"),
        "restore" => png!("restore"),
        "build" => png!("build"),
        "inspect" => png!("inspect"),
        "convert" => png!("convert"),
        "disc" => png!("disc"),
        "archive" => png!("archive"),
        "commander" => png!("commander"),
        _ => return None,
    })
}

fn icon(name: &str) -> Option<Retained<NSImage>> {
    let data = NSData::with_bytes(icon_png(name)?);
    let image = NSImage::initWithData(NSImage::alloc(), &data)?;
    image.setSize(NSSize::new(24.0, 24.0));
    image.setTemplate(true);
    Some(image)
}

fn make_item(mtm: MainThreadMarker, id: &NSString) -> Option<Retained<NSToolbarItem>> {
    let key = id.to_string();
    let (label, icon_name) = if key == ids::COMMANDER {
        ("Commander", "commander")
    } else {
        let tab = tab_of(&key)?;
        (tab.label(), tab.icon())
    };
    let item = NSToolbarItem::initWithItemIdentifier(mtm.alloc(), id);
    item.setLabel(&ns(label));
    item.setPaletteLabel(&ns(label));
    item.setToolTip(Some(&ns(label)));
    if let Some(image) = icon(icon_name) {
        item.setImage(Some(&image));
    }
    let t = target(mtm);
    // SAFETY: the target is never dropped.
    unsafe {
        item.setTarget(Some(&t));
        item.setAction(Some(sel!(act:)));
    }
    // Commander arrives in slice 3; until then its item shows but is off.
    if key == ids::COMMANDER {
        item.setAutovalidates(false);
        item.setEnabled(false);
    }
    Some(item)
}

define_class!(
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBToolbarDelegate"]
    pub struct Delegate;

    unsafe impl NSObjectProtocol for Delegate {}

    unsafe impl NSToolbarDelegate for Delegate {
        #[unsafe(method_id(toolbar:itemForItemIdentifier:willBeInsertedIntoToolbar:))]
        fn item(
            &self,
            _toolbar: &NSToolbar,
            id: &NSString,
            _insert: bool,
        ) -> Option<Retained<NSToolbarItem>> {
            make_item(self.mtm(), id)
        }

        #[unsafe(method_id(toolbarDefaultItemIdentifiers:))]
        fn defaults(&self, _toolbar: &NSToolbar) -> Retained<NSArray<NSString>> {
            all_ids()
        }

        #[unsafe(method_id(toolbarAllowedItemIdentifiers:))]
        fn allowed(&self, _toolbar: &NSToolbar) -> Retained<NSArray<NSString>> {
            all_ids()
        }

        #[unsafe(method_id(toolbarSelectableItemIdentifiers:))]
        fn selectable(&self, _toolbar: &NSToolbar) -> Retained<NSArray<NSString>> {
            let v: Vec<_> = AppTab::ALL.iter().map(|t| ns(&screen_id(*t))).collect();
            NSArray::from_retained_slice(&v)
        }
    }
);

fn all_ids() -> Retained<NSArray<NSString>> {
    let mut v: Vec<_> = AppTab::ALL.iter().map(|t| ns(&screen_id(*t))).collect();
    // SAFETY: a constant AppKit defines.
    v.push(unsafe { NSToolbarFlexibleSpaceItemIdentifier }.copy());
    v.push(ns(ids::COMMANDER));
    NSArray::from_retained_slice(&v)
}

/// The toolbar and its delegate, which the caller keeps alive (the toolbar holds it weakly).
pub fn build(mtm: MainThreadMarker) -> (Retained<NSToolbar>, Retained<Delegate>) {
    let delegate: Retained<Delegate> = {
        let this = mtm.alloc::<Delegate>().set_ivars(());
        // SAFETY: NSObject's plain init.
        unsafe { msg_send![super(this), init] }
    };
    let toolbar = NSToolbar::initWithIdentifier(mtm.alloc(), &ns("app.screens"));
    toolbar.setDelegate(Some(ProtocolObject::from_ref(&*delegate)));
    toolbar.setDisplayMode(NSToolbarDisplayMode::IconAndLabel);
    toolbar.setAllowsUserCustomization(false);
    (toolbar, delegate)
}

/// Show `tab` as the pressed screen.
pub fn select(toolbar: &NSToolbar, tab: AppTab) {
    let id = ns(&screen_id(tab));
    let same = toolbar.selectedItemIdentifier().is_some_and(|s| *s == *id);
    if !same {
        toolbar.setSelectedItemIdentifier(Some(&id));
    }
}
