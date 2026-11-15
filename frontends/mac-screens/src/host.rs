//! What a Mac shell's toolkit does for the screens: windows, the controls of a
//! [`Form`](super::form::Form), the effect host's panels, and the main-thread queue.
//!
//! The AppKit shell implements it with objc2; the vintage shell over a C ABI in Objective-C. A
//! surface is a view controls sit on (a window's content, a page, a tab); a control is a small
//! integer the form hands out. Every call is on the main thread, and no call may run a surface's
//! handler while the host holds a borrow the handler's calls back in would need. A question
//! (`pick`, `confirm`, ...) is modal: it returns the answer, and the app keeps running meanwhile.

use std::path::PathBuf;
use std::rc::Rc;

use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::keys::{Key, Mods};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{ChoiceSpec, ConfirmSpec, PasswordSpec, PickSpec, TextSpec};

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
    /// A list's selection changed by the user's hand.
    Selected,
    /// Return or a double-click on a list row.
    Activated,
    /// A list row's tick box changed; `toggled` says which row and how.
    Toggled,
}

/// One pull-down item: its label, the control it acts as (none for a caption), and enabled.
pub type PullItem = (String, Option<Ctl>, bool);

/// One list column: its title, its width in points, and whether it is right-aligned.
pub type Column = (String, f64, bool);

/// A colour a painter draws in: a fixed one, or the theme's for its role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ink {
    Rgb(u8, u8, u8),
    /// Body text.
    Label,
    /// A hairline between parts.
    Separator,
    /// Text on a coloured fill: white in dark mode, black in light.
    OnFill,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Align {
    Left,
    Center,
}

/// A canvas's drawing calls, valid only while its painter runs. Coordinates are flipped points.
pub trait Pen {
    fn fill(&self, x: f64, y: f64, w: f64, h: f64, ink: Ink);
    fn line(&self, x0: f64, y0: f64, x1: f64, y1: f64, ink: Ink);
    /// A rectangle's outline `width` points wide.
    fn frame(&self, x: f64, y: f64, w: f64, h: f64, width: f64, ink: Ink);
    /// Text in the small system font, cut with an ellipsis where it does not fit.
    /// `r` is `(x, y, w, h)`.
    fn text(&self, r: (f64, f64, f64, f64), text: &str, ink: Ink, align: Align);
    fn text_width(&self, text: &str) -> f64;
}

/// A listing row's icon (`assets/icons/rows`): a file or folder, and whether it opens and is open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowIcon {
    None,
    File {
        expandable: bool,
        expanded: bool,
    },
    Folder {
        expandable: bool,
        expanded: bool,
    },
    /// A folder-tree row: open or shut.
    TreeFolder {
        open: bool,
    },
}

/// One row of a pane list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowLine {
    pub cells: Vec<String>,
    /// Folder levels to indent the first cell by.
    pub depth: usize,
    pub icon: RowIcon,
    /// A tick box before the icon, and its state, when the list marks with them.
    pub tick: Option<bool>,
}

/// How a pane list looks: its columns and whether it has a header and takes several rows.
pub struct RowsSpec {
    pub columns: Vec<Column>,
    pub header: bool,
    pub multiple: bool,
    pub stripes: bool,
}

/// What the user did in a pane list.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RowsEvent {
    /// A click or an arrow key moved the selection to `row`, with Cmd and Shift as held.
    Select { row: usize, cmd: bool, shift: bool },
    /// The tick box of `row`.
    Tick(usize),
    /// The icon of `row`.
    Icon(usize),
    /// A double-click.
    Activate,
    /// A key the list does not use itself (arrows, paging and Home / End move its selection).
    Key(Key, Mods),
    /// A click on column `col`'s header.
    Header(usize),
    /// A right-click on `row` at `(x, y)` in the list.
    Menu { row: usize, x: f64, y: f64 },
    /// The list took the keyboard focus.
    Focused,
}

/// One entry of a context menu. Picks are numbered in the order entries appear, a submenu's
/// items in its place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MenuEntry {
    Item {
        label: String,
        enabled: bool,
        checked: bool,
    },
    Sub {
        label: String,
        items: Vec<String>,
    },
}

/// One toolbar item: the controller id it sends, its label, and its icon's name
/// (`assets/icons/toolbar`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolItem {
    pub id: String,
    pub label: String,
    pub icon: String,
}

/// Draws a canvas `w` by `h` points; `dark` is the view's appearance.
pub type Painter = Rc<dyn Fn(&dyn Pen, f64, f64, bool)>;

pub trait Host {
    /// Send every report from surface `s` to `on`, replacing any handler before.
    fn bind(&self, s: Surface, on: Rc<dyn Fn(Ctl, Ev)>);
    /// Drop surface `s`'s controls and handler; a window's surface also drops the window.
    fn release(&self, s: Surface);

    /// A titled, resizable window `w` by `h` points; its content is the surface returned.
    /// Closing it from its title bar asks `on_close`, which decides.
    fn window(&self, title: &str, w: i32, h: i32, on_close: Box<dyn Fn()>) -> Surface;
    /// Run `f` after the window's content is resized, to lay it out again.
    fn on_resize(&self, s: Surface, f: Box<dyn Fn()>);
    /// Show the window in front with nothing focused.
    fn present(&self, s: Surface);
    fn hide(&self, s: Surface);
    /// The surface's size as a layout rectangle at the origin.
    fn area(&self, s: Surface) -> Rect;
    /// Run the window modally until `stop_modal`; the app's events still flow.
    fn run_modal(&self, s: Surface);
    fn stop_modal(&self);

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
    /// Show the bar for a running job at `pct`, or sweeping when there is no percentage.
    fn set_progress(&self, s: Surface, c: Ctl, running: bool, pct: Option<u32>);
    /// A list in its scroll view; `checks` adds a first column of tick boxes.
    fn table(&self, s: Surface, c: Ctl, id: &str, columns: &[Column], checks: bool);
    /// Show `rows`; only a change reloads, and the selection survives it.
    fn set_rows(&self, s: Surface, c: Ctl, rows: &[Vec<String>]);
    fn set_checks(&self, s: Surface, c: Ctl, checks: &[bool]);
    fn selected_row(&self, s: Surface, c: Ctl) -> Option<usize>;
    /// Select row `i` as the screen's own change: it is not reported back.
    fn select_row(&self, s: Surface, c: Ctl, i: usize);
    /// The row and state of the last tick box the user changed.
    fn toggled(&self, s: Surface, c: Ctl) -> Option<(usize, bool)>;

    /// A view drawn by `painter`; a click reports `Act`, and `clicked` says where.
    fn canvas(&self, s: Surface, c: Ctl, id: &str, painter: Painter);
    fn redraw(&self, s: Surface, c: Ctl);
    /// The last click on a canvas and the canvas's size then: `(x, y, w, h)`.
    fn clicked(&self, s: Surface, c: Ctl) -> Option<(f64, f64, f64, f64)>;
    /// A read-only text view in a scroll view, in the fixed-width log font.
    fn text_view(&self, s: Surface, c: Ctl, id: &str);
    /// Replace its text; only a change redraws.
    fn set_text_view(&self, s: Surface, c: Ctl, text: &str);
    /// Add to its text, scrolling to the end when `follow`.
    fn append_text_view(&self, s: Surface, c: Ctl, text: &str, follow: bool);
    /// An editable plain-text area in the fixed-width font; typing reports `Edited`.
    fn text_area(&self, s: Surface, c: Ctl, id: &str, caption: &str);
    /// A text view's or area's text now.
    fn text_view_text(&self, s: Surface, c: Ctl) -> String;

    fn tooltip(&self, s: Surface, c: Ctl, text: &str);
    /// A control's size in points, to open a menu under it.
    fn control_size(&self, s: Surface, c: Ctl) -> (f64, f64);
    /// A pane list in its scroll view; what the user does goes to `on`.
    fn rows_view(&self, s: Surface, c: Ctl, id: &str, spec: &RowsSpec, on: Rc<dyn Fn(RowsEvent)>);
    /// Show `lines`; only a change reloads, so the selection and scroll survive.
    fn set_row_lines(&self, s: Surface, c: Ctl, lines: &[RowLine]);
    fn hide_column(&self, s: Surface, c: Ctl, col: usize, hidden: bool);
    /// Select `rows` as the screen's own change, scrolling `visible` into view.
    fn select_rows(&self, s: Surface, c: Ctl, rows: &[usize], visible: Option<usize>);
    /// Give the list the keyboard.
    fn focus(&self, s: Surface, c: Ctl);
    /// The rows one page of the list shows, for Page Up and Page Down.
    fn page_rows(&self, s: Surface, c: Ctl) -> i32;
    /// Show `items` at `(x, y)` in control `c` and wait; the entry picked, numbered as
    /// [`MenuEntry`] says.
    fn menu_at(&self, s: Surface, c: Ctl, items: &[MenuEntry], x: f64, y: f64) -> Option<usize>;
    /// Wrap a text view's lines at its width, or let them run on and scroll.
    fn wrap_text_view(&self, s: Surface, c: Ctl, wrap: bool);
    /// A window's frame on screen `(x, y, w, h)`, to reopen one where it was.
    fn window_frame(&self, s: Surface) -> Option<(f64, f64, f64, f64)>;
    fn set_window_frame(&self, s: Surface, frame: (f64, f64, f64, f64));
    /// Size a window's content and centre it on screen.
    fn resize_window(&self, s: Surface, w: i32, h: i32);
    /// The smallest a window's content may be made.
    fn set_min_size(&self, s: Surface, w: i32, h: i32);
    /// A scroll view as control `c` of `s`, its document the surface returned: a screen's page.
    fn page(&self, s: Surface, c: Ctl) -> Surface;
    /// A plain view as control `c` of `s`, as the surface returned: a page that stands in another's place.
    fn pane(&self, s: Surface, c: Ctl) -> Surface;
    /// The window's toolbar: the screens as selectable items, then `extra` (Commander) at the end.
    /// A pick reaches the app's command handler by the item's id, as a menu item does.
    fn toolbar(&self, s: Surface, items: &[ToolItem], extra: &str);
    fn select_tool(&self, s: Surface, id: &str);
    fn show_toolbar(&self, s: Surface, on: bool);
    /// Enable the menu bar's items by id: `on` says, or `None` leaves an item to its control.
    fn sync_menu(&self, on: &dyn Fn(&str) -> Option<bool>);
    /// Files dropped on window `s`, at `(x, y)` in its content.
    fn on_drop(&self, s: Surface, f: Rc<dyn Fn(Vec<PathBuf>, f64, f64)>);
    /// Whether `(x, y)` in window `s`'s content falls on control `c` of surface `on`.
    fn hit(&self, on: Surface, c: Ctl, x: f64, y: f64) -> bool;
    /// Where `Effect::Message` text goes: label `c` of `s`.
    fn status_to(&self, s: Surface, c: Ctl);
    /// The visible size of the scroll view a page's surface sits in, if it is in one.
    fn page_viewport(&self, s: Surface) -> Option<(i32, i32)>;
    /// Size a page's surface; taller than its viewport, it scrolls.
    fn set_surface_size(&self, s: Surface, w: i32, h: i32);
    /// The surface is on screen: neither it nor any view holding it is hidden.
    fn on_screen(&self, s: Surface) -> bool;

    fn remove(&self, s: Surface, c: Ctl);
    fn shown(&self, s: Surface, c: Ctl) -> bool;
    fn show(&self, s: Surface, c: Ctl, on: bool);
    fn enable(&self, s: Surface, c: Ctl, on: bool);
    fn checked(&self, s: Surface, c: Ctl) -> bool;
    fn set_check(&self, s: Surface, c: Ctl, on: bool);
    fn set_title(&self, s: Surface, c: Ctl, title: &str);
    /// The key that presses button `c`: "\r" for the default button, "\u{1b}" for Cancel.
    fn key_equivalent(&self, s: Surface, c: Ctl, key: &str);
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

    /// Run `f` on the main queue after the current event.
    fn later(&self, f: Box<dyn FnOnce()>);
    /// Run `f` on the main queue `ms` milliseconds from now.
    fn after(&self, ms: u64, f: Box<dyn FnOnce()>);
    /// Add lines to the app's log.
    fn log(&self, lines: Vec<(LogLevel, String)>);
    /// The transient message under the screens.
    fn status(&self, text: &str);
    /// The menu bar's recent files and devices, which mirror Inspect's source pull-down.
    fn menu_sources(&self, recent: &[String], devices: &[String]);
    fn open_url(&self, url: &str);
    fn copy_text(&self, text: &str);
    fn quit(&self);

    /// The open or save panel (`spec.save`); `None` when cancelled.
    fn pick(&self, spec: &PickSpec, folder: bool) -> Option<PathBuf>;
    /// An open panel that takes several files; none when cancelled.
    fn pick_files(&self, title: &str) -> Vec<PathBuf>;
    fn confirm(&self, spec: &ConfirmSpec) -> bool;
    /// A one-button note (About).
    fn note(&self, title: &str, text: &str);
    /// A question the shell itself asks: the button picked, or `None` for the Cancel it adds.
    fn ask(&self, title: &str, message: &str, buttons: &[String]) -> Option<usize>;
    /// The choice picked; Esc gives `spec.dismiss`.
    fn choose(&self, spec: &ChoiceSpec) -> usize;
    /// A one-line prompt that runs the spec's validator as the user types.
    fn ask_text(&self, spec: &TextSpec) -> Option<String>;
    fn ask_password(&self, spec: &PasswordSpec) -> Option<String>;
}
