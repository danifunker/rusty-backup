//! The listing's row icons (`assets/icons/rows`, Tabler) as template images, so they follow the
//! light or dark appearance; a folder with a + or - is the expander, as on Win32.

use objc2::rc::Retained;
use objc2::AllocAnyThread;
use objc2_app_kit::NSImage;
use objc2_foundation::{NSData, NSSize};

macro_rules! png {
    ($n:literal) => {
        include_bytes!(concat!("../../../assets/icons/rows/png/", $n, "-32.png"))
    };
}

/// The icon a listing row shows: whether it is a folder, can open, and is open.
pub fn for_row(is_dir: bool, expandable: bool, expanded: bool) -> Option<Retained<NSImage>> {
    let bytes: &[u8] = match (is_dir, expandable, expanded) {
        (true, true, false) => png!("folder-plus"),
        (true, true, true) => png!("folder-minus"),
        (true, false, _) => png!("folder"),
        (false, true, false) => png!("file-plus"),
        (false, true, true) => png!("file-minus"),
        (false, false, _) => png!("file"),
    };
    image(bytes)
}

/// A folder in the folder tree, open or shut.
pub fn folder(open: bool) -> Option<Retained<NSImage>> {
    image(if open {
        png!("folder-open")
    } else {
        png!("folder")
    })
}

fn image(bytes: &[u8]) -> Option<Retained<NSImage>> {
    let data = NSData::with_bytes(bytes);
    let image = NSImage::initWithData(NSImage::alloc(), &data)?;
    image.setSize(NSSize::new(16.0, 16.0));
    image.setTemplate(true);
    Some(image)
}
