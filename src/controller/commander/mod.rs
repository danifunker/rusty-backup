//! Commander: the two-pane file manager, as controllers every shell renders.
//!
//! Each pane is a [`crate::controller::browser::BrowserPaneController`] with the
//! [`PaneProfile::commander`] profile (re-exported here as `pane` and `rows`, the paths the
//! views already use). [`screen::CommanderController`] owns both panes and the work between
//! them (copy, export, checksums, File Info, the session log, keys). The egui Commander
//! (`src/gui/commander`) and the TUI Commander are shells on these.

pub mod ids;
pub mod screen;

pub use crate::controller::browser::{
    journal_block_class, journal_txn_classes, purpose, validate_entry_name, ApplyPreflight,
    ApplyReport, ArchiveRead, ArchiveRow, ArchiveSave, ArchiveUse, ArchiveViewer,
    BrowserPaneAction, BrowserPaneController, CheckSummary, DetailPlacement, DiscSource,
    DisplayRow, FailedEdit, ForkFormat, ImportChoice, JournalDetail, ListingStyle, MacArchiveKind,
    NoRecent, OnConflict, PaneProfile, PaneServices, RecentStore, RepairSummary, RowKind,
    SortColumn, SourceKind, StageOutcome, TarChoice, TarPreview, ToolReport, TreeText, VolumeInfo,
    MAX_CONSECUTIVE_FAILURES,
};
pub use crate::controller::browser::{metadata, pane, rows};
pub use screen::{
    default_export_format, export_choices, BudgetAction, BudgetForm, BudgetMode, ChecksumRow,
    Checksums, CommanderAction, CommanderController, DetailField, ExportChoice, ExportFormat,
    ExtractOptions, FileDetail, HashAlgo, IdNameMap, MetadataEdit, ModeAction, RenameNotice,
    ResourceForkMode, Side, SizePlan, SpaceReadout, UnsavedChoice, ViewItemKind, ViewMenuItem,
    Xattr, FOLLOW_DELAY, MIN_NESTED_IMAGE_BYTES,
};

// Plain data and helpers the views need, so they never name the model (CONTRIBUTING.md MVC).
pub use crate::controller::browser::{LineEnding, TextShape};
pub use crate::fs::entry::FileEntryTypeDisplay;
pub use fs_apple2::prodos_types::{
    is_known_type as is_known_prodos_type, type_description as prodos_type_description,
    UNKNOWN_TYPE_NOTE as PRODOS_UNKNOWN_TYPE_NOTE,
};

/// A preview's bytes as shown: QNX 2 text has its RS line separators turned into LF.
pub fn preview_bytes<'a>(fs_type: &str, data: &'a [u8]) -> std::borrow::Cow<'a, [u8]> {
    if fs_type == "QNX 2" && qnx2fs::looks_like_rs_text(data) {
        std::borrow::Cow::Owned(qnx2fs::rs_to_lf(data))
    } else {
        std::borrow::Cow::Borrowed(data)
    }
}
pub use crate::model::archive_edit::ArchiveEditContext;
pub use crate::model::browse_session::BrowseSession;
/// A Clonezilla partition's shared block cache, as the Inspect tab hands it over.
pub type PartcloneCache =
    std::sync::Arc<std::sync::Mutex<dsk_clonezilla::block_cache::PartcloneBlockCache>>;
pub use crate::model::edit_queue::EditQueue;
pub use crate::model::entry_detail::{detect_content_type, FileContent};
pub use crate::model::job::{JobRegistry, Snapshot};
pub use disk_core::display::format_size;
pub use disk_core::entry::{EntryType, FileEntry};

/// A blank 2 MiB FAT volume at `dir/vol.img`, for view tests.
#[cfg(test)]
pub(crate) fn blank_fat_image(dir: &std::path::Path) -> std::path::PathBuf {
    let img = dir.join("vol.img");
    let bytes = fs_pc::fat::create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
    std::fs::write(&img, bytes).unwrap();
    img
}
