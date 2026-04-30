//! Shared partition size-mode state.
//!
//! Three popups (restore, VHD-export, inspect-export) all let the user pick
//! how each partition is sized: keep the original, shrink to minimum, set a
//! custom size, or fill remaining space (restore only). They previously each
//! defined their own enum + `effective_size()` impl. This module unifies them.
//!
//! See `gui/size_mode_row.rs` for the matching widget.

/// Per-partition size choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SizeMode {
    #[default]
    Original,
    Minimum,
    Custom,
    /// Fill the remaining disk space. Only meaningful for the last partition
    /// in a restore plan; UIs that don't support it pass `allow_fill=false`
    /// to the widget. `effective_size` returns 0 here so callers compute the
    /// real value from the disk-size budget.
    FillRemaining,
}

impl SizeMode {
    /// Resolve to an effective byte size. `FillRemaining` returns 0 — the
    /// caller is expected to compute the actual fill value from the disk
    /// budget.
    pub fn effective_size(self, original: u64, minimum: u64, custom_mib: u32) -> u64 {
        match self {
            SizeMode::Original => original,
            SizeMode::Minimum => minimum,
            SizeMode::Custom => custom_mib as u64 * 1024 * 1024,
            SizeMode::FillRemaining => 0,
        }
    }
}
