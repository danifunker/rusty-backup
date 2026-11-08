//! Stable widget ids for Commander, which every shell sets as its accessibility identifier.

pub const LEFT_LIST: &str = "commander.left.list";
pub const RIGHT_LIST: &str = "commander.right.list";
pub const LEFT_APPLY: &str = "commander.left.apply";
pub const RIGHT_APPLY: &str = "commander.right.apply";
pub const COPY_LEFT_TO_RIGHT: &str = "commander.copy_ltr";
pub const COPY_RIGHT_TO_LEFT: &str = "commander.copy_rtl";
pub const DELETE: &str = "commander.delete";
pub const CLOSE: &str = "commander.close";
pub const VIEW_MENU: &str = "commander.view_menu";
pub const LOG: &str = "commander.log";
pub const STATUS: &str = "commander.status";
pub const COMPARE: &str = "commander.compare";
pub const KEEP_DATES: &str = "commander.keep_dates";
pub const EXPORT_FORMAT: &str = "commander.export_format";
pub const FORK_MODE: &str = "commander.fork_mode";
pub const CANCEL_COPY: &str = "commander.cancel_copy";

/// One pane's widgets, for the side it is on.
#[derive(Debug, Clone, Copy)]
pub struct PaneIds {
    pub list: &'static str,
    pub apply: &'static str,
    pub source: &'static str,
    pub recent: &'static str,
    pub open_file: &'static str,
    pub open_folder: &'static str,
    pub open_backup: &'static str,
    pub connect: &'static str,
    pub close: &'static str,
    pub close_image: &'static str,
    pub discard: &'static str,
    pub edits: &'static str,
    pub new_folder: &'static str,
    pub partition: &'static str,
    pub unlock: &'static str,
    pub cancel_open: &'static str,
}

pub const LEFT: PaneIds = PaneIds {
    list: LEFT_LIST,
    apply: LEFT_APPLY,
    source: "commander.left.source",
    recent: "commander.left.recent",
    open_file: "commander.left.open_file",
    open_folder: "commander.left.open_folder",
    open_backup: "commander.left.open_backup",
    connect: "commander.left.connect",
    close: "commander.left.close",
    close_image: "commander.left.close_image",
    discard: "commander.left.discard",
    edits: "commander.left.edits",
    new_folder: "commander.left.new_folder",
    partition: "commander.left.partition",
    unlock: "commander.left.unlock",
    cancel_open: "commander.left.cancel_open",
};

pub const RIGHT: PaneIds = PaneIds {
    list: RIGHT_LIST,
    apply: RIGHT_APPLY,
    source: "commander.right.source",
    recent: "commander.right.recent",
    open_file: "commander.right.open_file",
    open_folder: "commander.right.open_folder",
    open_backup: "commander.right.open_backup",
    connect: "commander.right.connect",
    close: "commander.right.close",
    close_image: "commander.right.close_image",
    discard: "commander.right.discard",
    edits: "commander.right.edits",
    new_folder: "commander.right.new_folder",
    partition: "commander.right.partition",
    unlock: "commander.right.unlock",
    cancel_open: "commander.right.cancel_open",
};
