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
    /// Minimum + 20% headroom, capped at `Original`. The default for the
    /// single-file CHD backup flow — gives the user some growth room without
    /// asking them to pick a number. UIs that don't surface it pass
    /// `allow_min_plus_20=false` to the widget.
    MinPlus20,
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
    /// budget. `MinPlus20` returns `min(minimum * 1.2, original)` rounded up
    /// to the next sector — never larger than the partition's source size.
    pub fn effective_size(self, original: u64, minimum: u64, custom_mib: u32) -> u64 {
        match self {
            SizeMode::Original => original,
            SizeMode::Minimum => minimum,
            SizeMode::MinPlus20 => min_plus_20(minimum, original),
            SizeMode::Custom => custom_mib as u64 * 1024 * 1024,
            SizeMode::FillRemaining => 0,
        }
    }
}

/// Compute Minimum + 20% headroom, capped at `original` and rounded up to a
/// 512-byte sector. Falls back to `original` if `minimum >= original`.
fn min_plus_20(minimum: u64, original: u64) -> u64 {
    if minimum >= original {
        return original;
    }
    // 20% headroom via integer math: minimum * 12 / 10. Saturating so a
    // pathological minimum near u64::MAX can't wrap.
    let bumped = minimum.saturating_add(minimum / 5).min(original);
    // Round up to the nearest 512-byte sector so partition layouts stay
    // sector-aligned without forcing the caller to do it.
    bumped.div_ceil(512).saturating_mul(512).min(original)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_plus_20_below_original_adds_headroom() {
        // 100 MiB minimum, 1 GiB original → ~120 MiB rounded to a sector.
        let minimum = 100 * 1024 * 1024;
        let original = 1024 * 1024 * 1024;
        let result = SizeMode::MinPlus20.effective_size(original, minimum, 0);
        assert_eq!(result, 120 * 1024 * 1024);
    }

    #[test]
    fn min_plus_20_caps_at_original() {
        // Minimum already > 83% of original — 20% headroom would overshoot.
        let minimum = 900;
        let original = 1000;
        let result = SizeMode::MinPlus20.effective_size(original, minimum, 0);
        assert_eq!(result, original);
    }

    #[test]
    fn min_plus_20_falls_back_when_minimum_equals_original() {
        let result = SizeMode::MinPlus20.effective_size(4096, 4096, 0);
        assert_eq!(result, 4096);
    }

    #[test]
    fn min_plus_20_rounds_up_to_sector() {
        // Minimum = 1000, +20% = 1200 → rounds up to 1536 (3 sectors).
        let result = SizeMode::MinPlus20.effective_size(u64::MAX, 1000, 0);
        assert_eq!(result, 1536);
    }
}
