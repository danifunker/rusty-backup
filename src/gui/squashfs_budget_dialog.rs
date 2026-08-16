//! "Size budget" dialog, shown before entering Edit Mode on a SquashFS volume.
//!
//! `docs/squashfs_edit.md` §2.6. Every other filesystem answers "will this
//! fit?" from a bitmap or a FAT; SquashFS has neither. It is densely packed
//! with no free space anywhere, committing an edit rebuilds the whole image,
//! and how large the result comes out depends on how the *new* content
//! compresses — which cannot be known in advance. So the user has to declare a
//! ceiling, and the point of this dialog is to make that a question they can
//! actually answer rather than a blind guess: it shows what the image occupies,
//! what it may occupy, and how well this image's own contents compressed.
//!
//! Refusing here is cheap. The alternative — discovering the answer after a
//! multi-minute rebuild, with the original already replaced — is the failure
//! mode the whole size budget exists to design out.

use rusty_backup::fs::squashfs_edit::{SizeBudget, SizePlan};
use rusty_backup::partition::format_size;

/// Which budget shape the user has selected.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Accept whatever the rebuild produces (the container still binds).
    Fit,
    /// An absolute ceiling.
    Absolute,
    /// Current size plus headroom.
    Headroom,
}

pub struct SquashfsBudgetDialog {
    plan: SizePlan,
    mode: Mode,
    /// MiB entered for the Absolute mode.
    absolute_mib: u64,
    /// MiB entered for the Headroom mode.
    headroom_mib: u64,
    /// Set once the user commits or cancels; the caller polls and acts.
    outcome: Option<Option<SizeBudget>>,
    closed: bool,
}

impl SquashfsBudgetDialog {
    /// Open the dialog for an already-measured volume.
    pub fn new(plan: SizePlan) -> Self {
        // Seed both inputs from where the image stands, so the numbers on
        // screen start out meaningful rather than at zero.
        let cur_mib = plan.image_len.div_ceil(1024 * 1024).max(1);
        Self {
            plan,
            // A bare file grows freely, so "fit" is the honest default there;
            // a partition already has a ceiling, and the default is to keep it.
            mode: Mode::Fit,
            absolute_mib: plan
                .capacity
                .map(|c| c / (1024 * 1024))
                .unwrap_or(cur_mib * 2)
                .max(1),
            headroom_mib: 64,
            outcome: None,
            closed: false,
        }
    }

    /// `Some(budget)` once the user has chosen — the inner `Option` is `None`
    /// for "no explicit request, let the container decide". `None` while the
    /// dialog is still open.
    pub fn take_outcome(&mut self) -> Option<Option<SizeBudget>> {
        self.outcome.take()
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// The budget the current selection represents.
    fn selected(&self) -> Option<SizeBudget> {
        const MIB: u64 = 1024 * 1024;
        match self.mode {
            Mode::Fit => Some(SizeBudget::Fit),
            Mode::Absolute => Some(SizeBudget::Limit(self.absolute_mib * MIB)),
            Mode::Headroom => Some(SizeBudget::Grow(self.headroom_mib * MIB)),
        }
    }

    /// What the selection resolves to as a hard ceiling, for the live readout.
    fn ceiling(&self) -> Option<u64> {
        self.selected()
            .and_then(|b| b.ceiling_for(self.plan.image_len))
    }

    pub fn show(&mut self, ctx: &egui::Context) {
        let mut open = !self.closed;
        egui::Window::new("SquashFS size budget")
            .open(&mut open)
            .resizable(false)
            .collapsible(false)
            .show(ctx, |ui| {
                ui.label(
                    "SquashFS has no free space to report and no in-place write: saving \
                     an edit rebuilds the whole image. Choose how large the result may be.",
                );
                ui.separator();

                egui::Grid::new("squashfs_budget_facts")
                    .num_columns(2)
                    .spacing([16.0, 4.0])
                    .show(ui, |ui| {
                        ui.label("Image occupies");
                        ui.label(format_size(self.plan.image_len));
                        ui.end_row();

                        ui.label("File content");
                        ui.label(format!(
                            "{} uncompressed",
                            format_size(self.plan.content_len)
                        ));
                        ui.end_row();

                        ui.label("Compressed to");
                        ui.label(self.plan.describe_ratio());
                        ui.end_row();

                        ui.label("Room available");
                        match (self.plan.capacity, self.plan.headroom) {
                            (Some(cap), Some(head)) => ui.label(format!(
                                "{} ({} unused)",
                                format_size(cap),
                                format_size(head)
                            )),
                            _ => ui.label("unbounded - this file is the filesystem"),
                        };
                        ui.end_row();
                    });

                ui.separator();
                ui.radio_value(
                    &mut self.mode,
                    Mode::Fit,
                    "Fit - accept whatever it becomes",
                );
                ui.horizontal(|ui| {
                    ui.radio_value(&mut self.mode, Mode::Absolute, "At most");
                    ui.add_enabled(
                        self.mode == Mode::Absolute,
                        egui::DragValue::new(&mut self.absolute_mib)
                            .speed(1.0)
                            .suffix(" MiB"),
                    );
                });
                ui.horizontal(|ui| {
                    ui.radio_value(&mut self.mode, Mode::Headroom, "Grow by at most");
                    ui.add_enabled(
                        self.mode == Mode::Headroom,
                        egui::DragValue::new(&mut self.headroom_mib)
                            .speed(1.0)
                            .suffix(" MiB"),
                    );
                });

                ui.separator();
                // The live readout, and the one refusal we can make before any
                // edit is attempted (stage 1 of the two-stage enforcement).
                let over_capacity = match (self.ceiling(), self.plan.capacity) {
                    (Some(want), Some(cap)) => want > cap,
                    _ => false,
                };
                match self.ceiling() {
                    Some(c) => ui.label(format!("Ceiling: {}", format_size(c))),
                    None => ui.label("Ceiling: none beyond what the container allows"),
                };
                if over_capacity {
                    ui.colored_label(
                        super::theme::danger(ui.visuals()),
                        format!(
                            "That is more than the {} available where this image lives.",
                            format_size(self.plan.capacity.unwrap_or(0))
                        ),
                    );
                }

                ui.separator();
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(!over_capacity, egui::Button::new("Start editing"))
                        .clicked()
                    {
                        self.outcome = Some(self.selected());
                        self.closed = true;
                    }
                    if ui.button("Cancel").clicked() {
                        self.closed = true;
                    }
                });
            });
        if !open {
            self.closed = true;
        }
    }
}
