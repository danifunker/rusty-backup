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
use rusty_backup::partition::format_size;

/// One block in the bar: a real partition, trailing unallocated space, or a
/// dimmed bookkeeping region (APM partition-map entry, SGI volume header).
#[derive(Debug, Clone)]
pub struct Segment {
    /// Human-readable name. For partitions: volume label, falling back to
    /// `Partition N` when no label is available. For free space: empty.
    pub label: String,
    /// Filesystem display name (e.g. "FAT16", "XFS", "ext4"). Empty for free
    /// space and dimmed regions where no FS applies.
    pub fs: String,
    pub size_bytes: u64,
    pub kind: SegmentKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    /// A real partition. `color_index` indexes into the fixed palette; callers
    /// typically assign sequentially across the disk so colors are stable.
    Partition { color_index: usize },
    /// Trailing unallocated space (or a gap between partitions). Rendered as
    /// solid neutral gray. Constructed by the planned-layout builder in
    /// Phase 2 of `docs/disk_expansion.md`; the inspect-tab passive view
    /// doesn't yet emit it.
    #[allow(dead_code)]
    Free,
    /// Bookkeeping region not owned by the user (APM partition-map entry,
    /// SGI volume header). Rendered dimmed.
    Dimmed,
}

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

        const BAR_HEIGHT: f32 = 34.0;
        const MIN_SEGMENT_PX: f32 = 6.0;
        const LABEL_THRESHOLD_PX: f32 = 60.0;

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

        // First pass: compute widths honoring the minimum-pip constraint while
        // keeping the total equal to the bar width. We start with proportional
        // widths, clamp anything below MIN_SEGMENT_PX up to it, then shrink
        // larger segments proportionally to absorb the deficit.
        let widths = compute_segment_widths(&self.segments, available_width, MIN_SEGMENT_PX);

        // Second pass: paint.
        let mut x = rect.left();
        let hover_pos = response.hover_pos();
        let mut hover_idx: Option<usize> = None;
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
                    egui::Stroke::new(1.0, ui.visuals().window_stroke.color),
                );
            }

            // Inline label when the segment is wide enough and the kind makes
            // sense to label. Free segments never get inline labels.
            if self.show_inline_labels
                && w >= LABEL_THRESHOLD_PX
                && !matches!(segment.kind, SegmentKind::Free)
            {
                let text = inline_label(segment);
                painter.text(
                    seg_rect.center(),
                    egui::Align2::CENTER_CENTER,
                    text,
                    egui::FontId::proportional(12.0),
                    inline_text_color(ui.visuals(), segment.kind),
                );
            }

            if let Some(pos) = hover_pos {
                if seg_rect.contains(pos) {
                    hover_idx = Some(i);
                }
            }
            x += w;
        }

        // Outer border.
        painter.rect_stroke(
            rect,
            egui::CornerRadius::same(2),
            egui::Stroke::new(1.0, ui.visuals().window_stroke.color),
            egui::StrokeKind::Inside,
        );

        if let Some(i) = hover_idx {
            let segment = &self.segments[i];
            response.on_hover_text(tooltip_text(segment));
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
                    egui::Stroke::new(1.0, ui.visuals().window_stroke.color),
                    egui::StrokeKind::Inside,
                );

                let text = legend_text(segment);
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

/// Fixed palette of partition colors. Cycles for disks with more partitions
/// than entries.
const PALETTE: &[egui::Color32] = &[
    egui::Color32::from_rgb(0x4c, 0x8c, 0xc4), // blue
    egui::Color32::from_rgb(0x6a, 0xb6, 0x4c), // green
    egui::Color32::from_rgb(0xc4, 0x88, 0x4c), // orange
    egui::Color32::from_rgb(0xb0, 0x5a, 0xa0), // magenta
    egui::Color32::from_rgb(0x4c, 0xb0, 0xb0), // teal
    egui::Color32::from_rgb(0xc4, 0xb4, 0x4c), // gold
    egui::Color32::from_rgb(0x8a, 0x6c, 0xc4), // violet
    egui::Color32::from_rgb(0xc4, 0x6c, 0x6c), // salmon
];

const FREE_COLOR: egui::Color32 = egui::Color32::from_rgb(0x9a, 0x9a, 0x9a);
const DIMMED_COLOR: egui::Color32 = egui::Color32::from_rgb(0x66, 0x66, 0x66);

fn segment_color(_visuals: &egui::Visuals, kind: SegmentKind) -> egui::Color32 {
    match kind {
        SegmentKind::Partition { color_index } => PALETTE[color_index % PALETTE.len()],
        SegmentKind::Free => FREE_COLOR,
        SegmentKind::Dimmed => DIMMED_COLOR,
    }
}

fn inline_text_color(_visuals: &egui::Visuals, _kind: SegmentKind) -> egui::Color32 {
    // Palette colors are mid-tone; white reads on all of them.
    egui::Color32::WHITE
}

fn inline_label(segment: &Segment) -> String {
    match (segment.label.is_empty(), segment.fs.is_empty()) {
        (false, false) => format!("{} [{}]", segment.label, segment.fs),
        (true, false) => format!("[{}]", segment.fs),
        (false, true) => segment.label.clone(),
        (true, true) => String::new(),
    }
}

fn legend_text(segment: &Segment) -> String {
    let body = match (segment.label.is_empty(), segment.fs.is_empty()) {
        (false, false) => format!("{} [{}]", segment.label, segment.fs),
        (true, false) => format!("[{}]", segment.fs),
        (false, true) => segment.label.clone(),
        (true, true) => match segment.kind {
            SegmentKind::Free => "Free".to_string(),
            _ => "Partition".to_string(),
        },
    };
    format!("{}  {}", body, format_size(segment.size_bytes))
}

fn tooltip_text(segment: &Segment) -> String {
    let mut lines = Vec::new();
    if !segment.label.is_empty() {
        lines.push(segment.label.clone());
    }
    if !segment.fs.is_empty() {
        lines.push(format!("Filesystem: {}", segment.fs));
    }
    lines.push(format!("Size: {}", format_size(segment.size_bytes)));
    match segment.kind {
        SegmentKind::Free => lines.push("Unallocated free space".to_string()),
        SegmentKind::Dimmed => lines.push("Bookkeeping region".to_string()),
        SegmentKind::Partition { .. } => {}
    }
    lines.join("\n")
}

/// Distribute `total_width` across the segments proportional to `size_bytes`,
/// clamping each segment to at least `min_px`. When the minimum clamps eat
/// into the total, the surplus is taken from segments above the minimum in
/// proportion to their (post-clamp-eligible) sizes. Returns one width per
/// input segment, summing exactly to `total_width` (within float epsilon).
fn compute_segment_widths(segments: &[Segment], total_width: f32, min_px: f32) -> Vec<f32> {
    let n = segments.len();
    if n == 0 {
        return Vec::new();
    }

    let total_bytes: u64 = segments.iter().map(|s| s.size_bytes).sum();
    if total_bytes == 0 {
        return vec![total_width / n as f32; n];
    }

    // Initial proportional widths.
    let mut widths: Vec<f32> = segments
        .iter()
        .map(|s| total_width * (s.size_bytes as f64 / total_bytes as f64) as f32)
        .collect();

    // If even the minimum-pip count exceeds total width, bail to equal split.
    if min_px * n as f32 > total_width {
        return vec![total_width / n as f32; n];
    }

    // Clamp small segments to min_px; track who got clamped and how much surplus
    // we need to reclaim from the unclamped.
    let mut clamped = vec![false; n];
    let mut deficit = 0.0_f32;
    for i in 0..n {
        if widths[i] < min_px {
            deficit += min_px - widths[i];
            widths[i] = min_px;
            clamped[i] = true;
        }
    }

    if deficit <= 0.0 {
        return widths;
    }

    // Shrink unclamped segments proportionally to absorb the deficit. Repeat
    // if shrinking pushes any below min_px (rare but possible on extreme
    // disks).
    loop {
        let unclamped_total: f32 = widths
            .iter()
            .zip(clamped.iter())
            .filter_map(|(w, c)| if !*c { Some(*w) } else { None })
            .sum();
        if unclamped_total <= 0.0 {
            break;
        }
        let scale = (unclamped_total - deficit) / unclamped_total;
        let mut new_deficit = 0.0_f32;
        for i in 0..n {
            if !clamped[i] {
                let new_w = widths[i] * scale;
                if new_w < min_px {
                    new_deficit += min_px - new_w;
                    widths[i] = min_px;
                    clamped[i] = true;
                } else {
                    widths[i] = new_w;
                }
            }
        }
        if new_deficit <= 0.0 {
            break;
        }
        deficit = new_deficit;
    }

    widths
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(size: u64) -> Segment {
        Segment {
            label: String::new(),
            fs: String::new(),
            size_bytes: size,
            kind: SegmentKind::Partition { color_index: 0 },
        }
    }

    #[test]
    fn widths_sum_to_total() {
        let segs = vec![mk(100_000_000), mk(600_000_000), mk(250_000_000)];
        let widths = compute_segment_widths(&segs, 800.0, 6.0);
        let sum: f32 = widths.iter().sum();
        assert!((sum - 800.0).abs() < 0.5, "widths sum {sum} != 800");
    }

    #[test]
    fn tiny_segments_clamped_to_min() {
        // Two partitions: one tiny EFI-sized (1 MB), one huge (10 GB).
        let segs = vec![mk(1_000_000), mk(10_000_000_000)];
        let widths = compute_segment_widths(&segs, 800.0, 6.0);
        assert!(widths[0] >= 6.0, "tiny segment not clamped: {}", widths[0]);
        let sum: f32 = widths.iter().sum();
        assert!((sum - 800.0).abs() < 0.5);
    }

    #[test]
    fn equal_split_when_overcrowded() {
        // 200 partitions in 800 px with 6px min would need 1200 px.
        let segs: Vec<Segment> = (0..200).map(|_| mk(1)).collect();
        let widths = compute_segment_widths(&segs, 800.0, 6.0);
        assert_eq!(widths.len(), 200);
        // Each should be ~4.0 (800/200).
        for w in &widths {
            assert!((w - 4.0).abs() < 0.1);
        }
    }

    #[test]
    fn legend_text_uses_volume_and_fs() {
        let seg = Segment {
            label: "C:".into(),
            fs: "FAT16".into(),
            size_bytes: 250_000_000,
            kind: SegmentKind::Partition { color_index: 0 },
        };
        let s = legend_text(&seg);
        assert!(s.contains("C:"));
        assert!(s.contains("FAT16"));
        assert!(s.contains("MB") || s.contains("MiB"));
    }

    #[test]
    fn legend_text_free_no_label() {
        let seg = Segment {
            label: String::new(),
            fs: String::new(),
            size_bytes: 1_024_000_000,
            kind: SegmentKind::Free,
        };
        let s = legend_text(&seg);
        assert!(s.contains("Free"));
    }
}
