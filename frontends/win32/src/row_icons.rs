//! The browse view's row icons: Tabler outlines in `assets/icons/rows`, rendered to PNGs by
//! `scripts/render-toolbar-icons.ps1 -Group rows`, white on a dark theme and black on a light one.
//!
//! A folder that opens in place carries a + while it is shut and a - while it is open, so the
//! icon is the expander; an archive or disk image that opens the same way does it on a page.

use rusty_backup::controller::commander::DisplayRow;
use windows::Win32::UI::Controls::{
    ImageList_Create, ImageList_ReplaceIcon, HIMAGELIST, ILC_COLOR32,
};
use windows::Win32::UI::WindowsAndMessaging::{
    CreateIconFromResourceEx, DestroyIcon, LR_DEFAULTCOLOR,
};

use crate::layout;

pub const FILE: i32 = 0;
pub const FOLDER_SHUT: i32 = 1;
pub const FOLDER_OPENED: i32 = 2;
pub const WRAPPER_SHUT: i32 = 3;
pub const WRAPPER_OPENED: i32 = 4;
pub const FOLDER: i32 = 5;
pub const FOLDER_OPEN: i32 = 6;

macro_rules! png {
    ($set:literal, $n:literal, $size:literal) => {
        include_bytes!(concat!(
            "../../../assets/icons/rows/",
            $set,
            "/",
            $n,
            "-",
            $size,
            ".png"
        ))
        .as_slice()
    };
}

macro_rules! sizes {
    ($set:literal, $n:literal) => {
        [
            (16, png!($set, $n, "16")),
            (24, png!($set, $n, "24")),
            (32, png!($set, $n, "32")),
        ]
    };
    ($n:literal) => {
        if crate::theme::dark() {
            sizes!("png-dark", $n)
        } else {
            sizes!("png", $n)
        }
    };
}

/// The icons in image-list order, each smallest first.
fn all() -> [[(i32, &'static [u8]); 3]; 7] {
    [
        sizes!("file"),
        sizes!("folder-plus"),
        sizes!("folder-minus"),
        sizes!("file-plus"),
        sizes!("file-minus"),
        sizes!("folder"),
        sizes!("folder-open"),
    ]
}

/// A new image list of every row icon at 16 DIPs for `dpi`; whoever takes it frees it.
pub fn image_list(dpi: u32) -> HIMAGELIST {
    let px = layout::scale(16, dpi);
    // SAFETY: a fresh image list; each icon is copied in and freed.
    unsafe {
        let il = ImageList_Create(px, px, ILC_COLOR32, 8, 0);
        for set in all() {
            let (_, bytes) = set
                .iter()
                .find(|(s, _)| *s >= px)
                .copied()
                .unwrap_or(set[2]);
            if let Ok(icon) =
                CreateIconFromResourceEx(bytes, true, 0x0003_0000, px, px, LR_DEFAULTCOLOR)
            {
                ImageList_ReplaceIcon(il, -1, icon);
                let _ = DestroyIcon(icon);
            }
        }
        il
    }
}

/// The icon a listing row shows.
pub fn for_row(row: &DisplayRow) -> i32 {
    match (row.is_dir, row.expandable, row.expanded) {
        (true, true, false) => FOLDER_SHUT,
        (true, true, true) => FOLDER_OPENED,
        (true, false, _) => FOLDER,
        (false, true, false) => WRAPPER_SHUT,
        (false, true, true) => WRAPPER_OPENED,
        (false, false, _) => FILE,
    }
}
