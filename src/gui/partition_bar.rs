//! Read-only horizontal partition-layout bar with a colored legend below.
//!
//! Used by the inspect tab's collapsible "Disk layout" section today, and
//! intended for reuse in the restore tab and the (planned) expand-disk dialog
//! once Phase 2 of `docs/disk_expansion.md` lands.
//!
//! The widget is pure visualization: no click selection, no drag handles.
//! Callers build a `Vec<Segment>` describing the disk layout and call
//! `PartitionBar::show`. The bar paints each segment proportional to its
//! `size_bytes`, with a minimum pip width so tiny partitions stay visible on
//! GPT/APM disks with many entries. Inline labels render only when a segment
//! is wide enough; otherwise the legend below carries the identification.

use eframe::egui;
use rusty_backup::controller::inspect::{segment_at, segment_widths, BAR_HEIGHT, MIN_SEGMENT_PX};

pub use rusty_backup::controller::inspect::{LayoutSegment as Segment, SegmentKind};

/// Reusable partition-layout bar widget.
pub struct PartitionBar {
    pub segments: Vec<Segment>,
    /// Render `<label> [<fs>]` inside segments wide enough to fit them.
    pub show_inline_labels: bool,
    /// Render the legend rows below the bar.
    pub show_legend: bool,
}

impl PartitionBar {
    pub fn new(segments: Vec<Segment>) -> Self {
        Self {
            segments,
            show_inline_labels: true,
            show_legend: true,
        }
    }

    /// Paint the bar and legend into `ui`. Uses the full available width.
    pub fn show(&self, ui: &mut egui::Ui) {
        let total_bytes: u64 = self.segments.iter().map(|s| s.size_bytes).sum();
        if total_bytes == 0 || self.segments.is_empty() {
            ui.label("(no partitions to display)");
            return;
        }

        let available_width = ui.available_width().max(120.0);
        let (rect, response) = ui.allocate_exact_size(
            egui::vec2(available_width, BAR_HEIGHT),
            egui::Sense::hover(),
        );
        let painter = ui.painter_at(rect);

        // Bar background + border.
        painter.rect_filled(
            rect,
            egui::CornerRadius::same(2),
            ui.visuals().extreme_bg_color,
        );

        let widths = segment_widths(&self.segments, available_width, MIN_SEGMENT_PX);

        let mut x = rect.left();
        let hover_idx = response
            .hover_pos()
            .and_then(|pos| segment_at(&widths, pos.x - rect.left()));
        for (i, segment) in self.segments.iter().enumerate() {
            let w = widths[i];
            let seg_rect =
                egui::Rect::from_min_size(egui::pos2(x, rect.top()), egui::vec2(w, rect.height()));
            let color = segment_color(ui.visuals(), segment.kind);
            painter.rect_filled(seg_rect, egui::CornerRadius::ZERO, color);

            // Vertical separator between segments (except after the last).
            if i + 1 < self.segments.len() {
                let sep_x = seg_rect.right();
                painter.line_segment(
                    [
                        egui::pos2(sep_x, rect.top()),
                        egui::pos2(sep_x, rect.bottom()),
                    ],
                    egui::Stroke::new(1.0_f32, ui.visuals().window_stroke.color),
                );
            }

            if let Some(text) = segment.inline_label(w).filter(|_| self.show_inline_labels) {
                painter.text(
                    seg_rect.center(),
                    egui::Align2::CENTER_CENTER,
                    text,
                    egui::FontId::proportional(12.0),
                    inline_text_color(ui.visuals(), segment.kind),
                );
            }

            x += w;
        }

        // Outer border.
        painter.rect_stroke(
            rect,
            egui::CornerRadius::same(2),
            egui::Stroke::new(1.0_f32, ui.visuals().window_stroke.color),
            egui::StrokeKind::Inside,
        );

        if let Some(i) = hover_idx {
            let segment = &self.segments[i];
            response.on_hover_text(segment.tooltip_text());
        }

        if self.show_legend {
            ui.add_space(4.0);
            self.show_legend_rows(ui);
        }
    }

    fn show_legend_rows(&self, ui: &mut egui::Ui) {
        ui.horizontal_wrapped(|ui| {
            for segment in &self.segments {
                let swatch_color = segment_color(ui.visuals(), segment.kind);
                let (rect, _) =
                    ui.allocate_exact_size(egui::vec2(12.0, 12.0), egui::Sense::hover());
                ui.painter()
                    .rect_filled(rect, egui::CornerRadius::same(2), swatch_color);
                ui.painter().rect_stroke(
                    rect,
                    egui::CornerRadius::same(2),
                    egui::Stroke::new(1.0_f32, ui.visuals().window_stroke.color),
                    egui::StrokeKind::Inside,
                );

                let text = segment.legend_text();
                match segment.kind {
                    SegmentKind::Dimmed => {
                        ui.label(egui::RichText::new(text).italics().weak());
                    }
                    SegmentKind::Free => {
                        ui.label(egui::RichText::new(text).weak());
                    }
                    SegmentKind::Partition { .. } => {
                        ui.label(text);
                    }
                }
                ui.add_space(8.0);
            }
        });
    }
}

fn segment_color(visuals: &egui::Visuals, kind: SegmentKind) -> egui::Color32 {
    match kind {
        SegmentKind::Partition { color_index } => {
            super::theme::partition_fill(visuals, color_index)
        }
        SegmentKind::Free => super::theme::free_space(visuals),
        SegmentKind::Dimmed => super::theme::dimmed(visuals),
    }
}

fn inline_text_color(visuals: &egui::Visuals, _kind: SegmentKind) -> egui::Color32 {
    super::theme::on_partition(visuals)
}
