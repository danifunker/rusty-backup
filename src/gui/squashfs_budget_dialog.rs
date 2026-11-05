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
//! mode the whole size budget exists to design out. The form and its rules are
//! `controller::commander::BudgetForm`; this draws it and returns the frame's edits.

use rusty_backup::controller::commander::{format_size, BudgetAction, BudgetForm, BudgetMode};

/// Draw the dialog for `form`; returns the edits, Start or Cancel made this frame.
pub fn show(ctx: &egui::Context, form: &BudgetForm) -> Vec<BudgetAction> {
    let mut out = Vec::new();
    let plan = *form.plan();
    let mut open = true;
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
                    ui.label(format_size(plan.image_len));
                    ui.end_row();

                    ui.label("File content");
                    ui.label(format!("{} uncompressed", format_size(plan.content_len)));
                    ui.end_row();

                    ui.label("Compressed to");
                    ui.label(plan.describe_ratio());
                    ui.end_row();

                    ui.label("Room available");
                    match (plan.capacity, plan.headroom) {
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
            let mode = form.mode();
            if ui
                .radio(mode == BudgetMode::Fit, "Fit - accept whatever it becomes")
                .clicked()
            {
                out.push(BudgetAction::SetMode(BudgetMode::Fit));
            }
            ui.horizontal(|ui| {
                if ui.radio(mode == BudgetMode::Absolute, "At most").clicked() {
                    out.push(BudgetAction::SetMode(BudgetMode::Absolute));
                }
                let mut mib = form.absolute_mib();
                let field = egui::DragValue::new(&mut mib).speed(1.0).suffix(" MiB");
                if ui
                    .add_enabled(mode == BudgetMode::Absolute, field)
                    .changed()
                {
                    out.push(BudgetAction::SetAbsoluteMib(mib));
                }
            });
            ui.horizontal(|ui| {
                if ui
                    .radio(mode == BudgetMode::Headroom, "Grow by at most")
                    .clicked()
                {
                    out.push(BudgetAction::SetMode(BudgetMode::Headroom));
                }
                let mut mib = form.headroom_mib();
                let field = egui::DragValue::new(&mut mib).speed(1.0).suffix(" MiB");
                if ui
                    .add_enabled(mode == BudgetMode::Headroom, field)
                    .changed()
                {
                    out.push(BudgetAction::SetHeadroomMib(mib));
                }
            });

            ui.separator();
            let over_capacity = form.over_capacity();
            match form.ceiling() {
                Some(c) => ui.label(format!("Ceiling: {}", format_size(c))),
                None => ui.label("Ceiling: none beyond what the container allows"),
            };
            if over_capacity {
                ui.colored_label(
                    super::theme::danger(ui.visuals()),
                    format!(
                        "That is more than the {} available where this image lives.",
                        format_size(plan.capacity.unwrap_or(0))
                    ),
                );
            }

            ui.separator();
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(!over_capacity, egui::Button::new("Start editing"))
                    .clicked()
                {
                    out.push(BudgetAction::Start);
                }
                if ui.button("Cancel").clicked() {
                    out.push(BudgetAction::Cancel);
                }
            });
        });
    if !open {
        out.push(BudgetAction::Cancel);
    }
    out
}
