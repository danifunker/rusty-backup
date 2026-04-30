//! Shared "size mode" + "size (MiB)" cell pair for partition-sizing grids.
//!
//! Used by the restore tab, the inspect-tab export popup, and the backup-tab
//! VHD popup. Each call site previously hand-rolled the same radio set +
//! DragValue + effective-size label. The widget emits two grid cells in
//! sequence (no `ui.end_row()`); callers wrap with their own header cells.
//!
//! See `model::size_mode::SizeMode` for the enum.

use rusty_backup::model::size_mode::SizeMode;

/// Optional features per call site. The widget always shows Original + Custom;
/// Minimum is shown when `minimum_size < original_size`; FillRemaining when
/// `allow_fill`; the deferred Calc-min button when `deferred` is set.
pub struct SizeModeRowOptions<'a> {
    /// Show a `FillRemaining` radio (restore last-partition only).
    pub allow_fill: bool,
    /// Upper bound on Custom DragValue. `None` = 2 TiB default.
    pub max_size: Option<u64>,
    /// Tooltip shown on the DragValue when `max_size` is set.
    pub max_size_hover: Option<&'a str>,
    /// State of an expensive-FS minimum-size calculation. When `Some`, replaces
    /// (or appends to) the radio row with a button or spinner.
    pub deferred: Option<DeferredMin<'a>>,
    /// Which size to seed the Custom DragValue with when the user first
    /// switches into Custom from another mode.
    pub custom_seed: CustomSeed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CustomSeed {
    #[default]
    Minimum,
    Original,
}

impl<'a> Default for SizeModeRowOptions<'a> {
    fn default() -> Self {
        Self {
            allow_fill: false,
            max_size: None,
            max_size_hover: None,
            deferred: None,
            custom_seed: CustomSeed::Minimum,
        }
    }
}

/// Whether the partition's minimum size is computed, pending, or awaiting a
/// user-initiated calc.
pub enum DeferredMin<'a> {
    /// Show "Calc min" button. Click sets the action to `CalcMinRequested`.
    Available { fs_name: &'a str },
    /// Show a spinner with the latest phase string as tooltip.
    Pending { phase: &'a str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeModeRowAction {
    None,
    /// User clicked the "Calc min" button — caller should spawn the worker.
    CalcMinRequested,
}

/// Render the "Size Mode" cell (radios + optional Calc-min button) followed
/// by the "Size (MiB)" cell (DragValue when Custom, label otherwise).
///
/// Mutates `choice` and `custom_mib`. Returns whether the user requested a
/// minimum-size calculation.
pub fn size_mode_row(
    ui: &mut egui::Ui,
    choice: &mut SizeMode,
    custom_mib: &mut u32,
    original_size: u64,
    minimum_size: u64,
    opts: SizeModeRowOptions<'_>,
) -> SizeModeRowAction {
    let prev_choice = *choice;
    let mut action = SizeModeRowAction::None;

    ui.horizontal(|ui| {
        ui.radio_value(choice, SizeMode::Original, "Original");
        if minimum_size < original_size {
            ui.radio_value(choice, SizeMode::Minimum, "Minimum");
        } else {
            match &opts.deferred {
                Some(DeferredMin::Pending { phase }) => {
                    ui.add(egui::Spinner::new()).on_hover_text(*phase);
                }
                Some(DeferredMin::Available { fs_name }) => {
                    if ui
                        .small_button("Calc min")
                        .on_hover_text(
                            format!("Compute minimum size (walks the {fs_name} volume)",),
                        )
                        .clicked()
                    {
                        action = SizeModeRowAction::CalcMinRequested;
                    }
                }
                None => {}
            }
        }
        ui.radio_value(choice, SizeMode::Custom, "Custom");
        if opts.allow_fill {
            ui.radio_value(choice, SizeMode::FillRemaining, "Fill");
        }
    });

    // When switching INTO Custom, seed the MiB value.
    if *choice == SizeMode::Custom && prev_choice != SizeMode::Custom {
        let seed_bytes = match opts.custom_seed {
            CustomSeed::Minimum => minimum_size,
            CustomSeed::Original => original_size,
        };
        *custom_mib = (seed_bytes / (1024 * 1024)).max(1) as u32;
    }

    match *choice {
        SizeMode::Custom => {
            let min_mib = (minimum_size / (1024 * 1024)).max(1) as u32;
            let max_mib = opts
                .max_size
                .map(|b| (b / (1024 * 1024)).max(min_mib as u64) as u32)
                .unwrap_or(2_097_152u32); // 2 TiB default ceiling
            let resp = ui.add(egui::DragValue::new(custom_mib).range(min_mib..=max_mib));
            if let Some(hover) = opts.max_size_hover {
                resp.on_hover_text(hover);
            }
        }
        SizeMode::FillRemaining => {
            ui.label("(auto)");
        }
        _ => {
            let bytes = choice.effective_size(original_size, minimum_size, *custom_mib);
            ui.label(format!("{}", bytes / (1024 * 1024)));
        }
    }

    action
}
