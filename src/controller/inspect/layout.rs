//! What every Inspect view draws alike: the disk-layout bar's maths and text, and the partition rows.

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use disk_core::display::format_size;

use super::{InspectController, MinCell, RowActions};

/// The bar's height, in the view's pixels.
pub const BAR_HEIGHT: f32 = 34.0;
/// The narrowest a segment gets, so a tiny partition stays visible.
pub const MIN_SEGMENT_PX: f32 = 6.0;
/// The narrowest segment that carries an inline label.
pub const LABEL_THRESHOLD_PX: f32 = 60.0;

/// Hover texts the Inspect views share.
pub mod hints {
    pub const NUMBER: &str = "Position in this list - the number `IMG@N` takes";
    pub const SLOT: &str = "The partition table's own slot, as the platform names it \
                            (diskutil's disk4s6, IRIX slot 0) - the number `IMG@sN` takes";
    pub const MIN_SIZE: &str = "Smallest size achievable by trim alone (no data movement)";
    pub const DEFRAG_MIN: &str = "Smallest size achievable by cloning into a packed target. \
                                  Used when restoring with \"Resize partitions to minimum size.\"";
    pub const CALC_MIN: &str = "Compute minimum size (requires a filesystem walk)";
    pub const MAKE_BOOTABLE: &str =
        "Auto-detect what this Mac disk needs to boot (SCSI driver + DDR on a \
         full APM disk, boot blocks, blessed System Folder) and apply only the \
         missing pieces. A flat HFV is kept flat; you'll be asked for a donor \
         disk only if boot blocks are missing.";
    pub const SWAP_WORD_ORDER: &str =
        "Write a copy of this image with the two bytes of every 16-bit word \
         exchanged, fixing a capture taken through a controller that reverses \
         words. The operation is its own inverse.";
    pub const DEFRAGMENT: &str = "Repack this Human68k volume so its files are stored \
                                  contiguously, reclaiming holes left by deleted files. \
                                  Rewrites the partition in place - back up first.";
}

/// One segment of a disk-layout bar; the view picks the colours.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayoutSegment {
    /// The volume label, else `Partition N`; empty for free space.
    pub label: String,
    /// The filesystem's display name; empty where none applies.
    pub fs: String,
    pub size_bytes: u64,
    pub kind: SegmentKind,
    /// The partition this segment shows, when a click on it can select one.
    pub partition: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    /// A volume; `color_index` picks from the view's palette, which it wraps around.
    Partition {
        color_index: usize,
    },
    /// A map or volume-header entry, not a volume.
    Dimmed,
    Free,
}

impl LayoutSegment {
    /// `label [fs]`, or as much of it as there is.
    fn name(&self) -> String {
        match (self.label.is_empty(), self.fs.is_empty()) {
            (false, false) => format!("{} [{}]", self.label, self.fs),
            (true, false) => format!("[{}]", self.fs),
            (false, true) => self.label.clone(),
            (true, true) => String::new(),
        }
    }

    /// The text inside the segment at `width`, when there is room and it is not free space.
    pub fn inline_label(&self, width: f32) -> Option<String> {
        let fits = width >= LABEL_THRESHOLD_PX && !matches!(self.kind, SegmentKind::Free);
        fits.then(|| self.name())
    }

    pub fn legend_text(&self) -> String {
        let name = self.name();
        let body = match (name.is_empty(), self.kind) {
            (false, _) => name,
            (true, SegmentKind::Free) => "Free".to_string(),
            (true, _) => "Partition".to_string(),
        };
        format!("{}  {}", body, format_size(self.size_bytes))
    }

    pub fn tooltip_text(&self) -> String {
        let mut lines = Vec::new();
        if !self.label.is_empty() {
            lines.push(self.label.clone());
        }
        if !self.fs.is_empty() {
            lines.push(format!("Filesystem: {}", self.fs));
        }
        lines.push(format!("Size: {}", format_size(self.size_bytes)));
        match self.kind {
            SegmentKind::Free => lines.push("Unallocated free space".to_string()),
            SegmentKind::Dimmed => lines.push("Bookkeeping region".to_string()),
            SegmentKind::Partition { .. } => {}
        }
        lines.join("\n")
    }
}

/// Spread `total_width` over the segments by size, each at least `min_px`, summing to the total.
pub fn segment_widths(segments: &[LayoutSegment], total_width: f32, min_px: f32) -> Vec<f32> {
    let n = segments.len();
    if n == 0 {
        return Vec::new();
    }
    let total_bytes: u64 = segments.iter().map(|s| s.size_bytes).sum();
    if total_bytes == 0 || min_px * n as f32 > total_width {
        return vec![total_width / n as f32; n];
    }
    let mut widths: Vec<f32> = segments
        .iter()
        .map(|s| total_width * (s.size_bytes as f64 / total_bytes as f64) as f32)
        .collect();

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

    // Shrinking the rest can push one below the minimum, so repeat until nothing moves.
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

/// Which of the laid-out segments holds `x`, measured from the bar's left edge.
pub fn segment_at(widths: &[f32], x: f32) -> Option<usize> {
    if x < 0.0 {
        return None;
    }
    let mut right = 0.0_f32;
    for (i, w) in widths.iter().enumerate() {
        right += w;
        if x < right {
            return Some(i);
        }
    }
    None
}

/// Base-1000 units, the way storage media is marketed ("8 GB" = 8,000,000,000 bytes).
pub fn format_size_decimal(bytes: u64) -> String {
    const GB: f64 = 1_000_000_000.0;
    const MB: f64 = 1_000_000.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else {
        format!("{:.1} MB", b / MB)
    }
}

/// A byte count with digit grouping (1,048,576).
pub fn format_bytes_grouped(bytes: u64) -> String {
    let s = bytes.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

/// A summary's size line: "1.5 MB (1,500,000 bytes)".
pub fn format_size_exact(bytes: u64) -> String {
    format!(
        "{} ({} bytes)",
        format_size_decimal(bytes),
        format_bytes_grouped(bytes)
    )
}

/// An HFS allocation block size: "N KiB" when a whole number of KiB, else "N B".
pub fn format_block_size(bytes: u32) -> String {
    if bytes >= 1024 && bytes.is_multiple_of(1024) {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{} B", bytes)
    }
}

/// One line of the partition list, as every view shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRow {
    /// The partition's own index, which the row's actions take.
    pub index: usize,
    /// The list position the user types back as `@N`, with its logical or container note.
    pub number: String,
    pub slot: String,
    pub type_name: String,
    /// Probed live from the partition, never from backup metadata.
    pub volume: String,
    pub start_lba: u64,
    pub size_bytes: u64,
    pub min: MinCell,
    pub defrag_min: Option<u64>,
    pub block_size: String,
    pub bootable: bool,
    /// An extended container: a muted row with nothing to act on.
    pub container: bool,
    pub actions: RowActions,
    /// A defragment rewrites the image under the rows.
    pub locked: bool,
}

impl InspectController {
    /// The partition list's rows, in list order.
    pub fn rows(&self) -> Vec<PartitionRow> {
        let locked = self.rows_locked();
        let rows = self.partitions().iter().enumerate().map(|(pos, part)| {
            let container = part.is_extended_container;
            let number = if part.is_logical {
                format!("  {} (logical)", pos + 1)
            } else if container {
                format!("{} (container)", pos + 1)
            } else {
                format!("{}", pos + 1)
            };
            let volume = (!container)
                .then(|| self.volume_label(part.index))
                .flatten()
                .unwrap_or("");
            let block_size = part.hfs_block_size.filter(|_| !container);
            PartitionRow {
                index: part.index,
                number,
                slot: self.slot(part),
                type_name: part.type_name.clone(),
                volume: volume.to_string(),
                start_lba: part.start_lba,
                size_bytes: part.size_bytes,
                min: self.min_cell(part),
                defrag_min: self.defrag_min(part),
                block_size: block_size.map(format_block_size).unwrap_or_default(),
                bootable: part.bootable && !container,
                container,
                actions: self.row_actions(part),
                locked,
            }
        });
        rows.collect()
    }

    /// The partition the view has selected, by its index.
    pub fn selected_partition(&self) -> Option<usize> {
        self.found.selected
    }

    /// Select a partition by index, or clear the selection; an index not listed clears it too.
    pub(super) fn select_partition(&mut self, index: Option<usize>) {
        let listed = |i: &usize| self.found.partitions.iter().any(|p| p.index == *i);
        self.found.selected = index.filter(listed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(size: u64) -> LayoutSegment {
        LayoutSegment {
            label: String::new(),
            fs: String::new(),
            size_bytes: size,
            kind: SegmentKind::Partition { color_index: 0 },
            partition: None,
        }
    }

    #[test]
    fn widths_sum_to_total() {
        let segs = vec![mk(100_000_000), mk(600_000_000), mk(250_000_000)];
        let widths = segment_widths(&segs, 800.0, 6.0);
        let sum: f32 = widths.iter().sum();
        assert!((sum - 800.0).abs() < 0.5, "widths sum {sum} != 800");
    }

    #[test]
    fn tiny_segments_clamped_to_min() {
        let segs = vec![mk(1_000_000), mk(10_000_000_000)];
        let widths = segment_widths(&segs, 800.0, 6.0);
        assert!(widths[0] >= 6.0, "tiny segment not clamped: {}", widths[0]);
        let sum: f32 = widths.iter().sum();
        assert!((sum - 800.0).abs() < 0.5);
    }

    #[test]
    fn equal_split_when_overcrowded() {
        let segs: Vec<LayoutSegment> = (0..200).map(|_| mk(1)).collect();
        let widths = segment_widths(&segs, 800.0, 6.0);
        assert_eq!(widths.len(), 200);
        for w in &widths {
            assert!((w - 4.0).abs() < 0.1);
        }
    }

    #[test]
    fn a_click_lands_in_the_segment_under_it() {
        let widths = [100.0, 50.0, 10.0];
        assert_eq!(segment_at(&widths, 0.0), Some(0));
        assert_eq!(segment_at(&widths, 99.9), Some(0));
        assert_eq!(segment_at(&widths, 100.0), Some(1));
        assert_eq!(segment_at(&widths, 159.9), Some(2));
        assert_eq!(segment_at(&widths, 160.0), None);
        assert_eq!(segment_at(&widths, -1.0), None);
    }

    #[test]
    fn legend_text_uses_volume_and_fs() {
        let seg = LayoutSegment {
            label: "C:".into(),
            fs: "FAT16".into(),
            ..mk(250_000_000)
        };
        let s = seg.legend_text();
        assert!(s.contains("C:"));
        assert!(s.contains("FAT16"));
        assert!(s.contains("MB") || s.contains("MiB"));
    }

    #[test]
    fn legend_text_free_no_label() {
        let seg = LayoutSegment {
            kind: SegmentKind::Free,
            ..mk(1_024_000_000)
        };
        assert!(seg.legend_text().contains("Free"));
    }

    #[test]
    fn only_a_wide_volume_carries_an_inline_label() {
        let seg = LayoutSegment {
            label: "C:".into(),
            ..mk(1)
        };
        assert_eq!(seg.inline_label(59.0), None);
        assert_eq!(seg.inline_label(60.0).as_deref(), Some("C:"));
        let free = LayoutSegment {
            kind: SegmentKind::Free,
            ..seg
        };
        assert_eq!(free.inline_label(600.0), None);
    }

    #[test]
    fn sizes_read_the_way_the_summary_prints_them() {
        assert_eq!(format_bytes_grouped(1_048_576), "1,048,576");
        assert_eq!(format_size_exact(1_500_000), "1.5 MB (1,500,000 bytes)");
        assert_eq!(format_block_size(4096), "4 KiB");
        assert_eq!(format_block_size(1536), "1536 B");
    }
}
