//! Read-only HFS+ journal history viewer (Phase 9, Step 29 of
//! `docs/hfsplus_enhancements.md`).
//!
//! Shows the parsed journal transaction log so the user can see what edit-mode
//! (or macOS) wrote, in order, with enough detail to troubleshoot a volume that
//! won't mount. ASCII only — no emoji or arrows beyond `->` (CLAUDE.md rule).

use rusty_backup::fs::hfsplus_journal::{classify_target, JournalDetail};

/// Modal state for the journal viewer. Created when the user clicks "Journal"
/// in the browse-view toolbar; holds the decoded journal plus the currently
/// selected transaction row.
pub struct JournalView {
    detail: JournalDetail,
    /// Index into `detail.transactions` of the expanded row, if any.
    selected: Option<usize>,
}

impl JournalView {
    pub fn new(detail: JournalDetail) -> Self {
        JournalView {
            detail,
            selected: None,
        }
    }

    /// Render the viewer window. Returns `false` once the user closes it so the
    /// caller can drop the view.
    pub fn show(&mut self, ctx: &egui::Context) -> bool {
        let mut open = true;
        egui::Window::new("HFS+ Journal")
            .open(&mut open)
            .resizable(true)
            .default_width(640.0)
            .default_height(480.0)
            .show(ctx, |ui| {
                self.render(ui);
            });
        open
    }

    fn render(&mut self, ui: &mut egui::Ui) {
        let h = &self.detail.header;
        let info = &self.detail.info;

        // ---- Header summary ------------------------------------------------
        ui.label(egui::RichText::new("Volume journal").strong());
        let location = if info.is_external() {
            "external device"
        } else {
            "in filesystem"
        };
        ui.label(format!(
            "Location: {location}    Journal offset: {}    Size: {}",
            human_bytes(info.offset),
            human_bytes(info.size),
        ));
        let wrap = if h.end < h.start { "  (wrapped)" } else { "" };
        ui.label(format!(
            "start: {}    end: {}{wrap}    sequence: {}    block size: {}",
            h.start, h.end, h.sequence_num, h.jhdr_size,
        ));

        let dirty = h.start != h.end;
        if dirty {
            ui.colored_label(
                egui::Color32::from_rgb(255, 200, 100),
                format!(
                    "Warning: journal is dirty - {} pending transaction(s) not yet \
                     applied to the volume. macOS would replay these on mount.",
                    self.detail.transactions.len()
                ),
            );
        } else {
            ui.colored_label(
                egui::Color32::from_rgb(100, 200, 100),
                "Journal is clean (empty) - nothing pending.",
            );
        }

        // ---- Pre-flight findings ------------------------------------------
        if let Some(seq) = self.detail.checksum_mismatch {
            ui.colored_label(
                egui::Color32::from_rgb(255, 100, 100),
                format!(
                    "Warning: transaction sequence {seq} has a bad checksum - \
                     macOS will stop replay here."
                ),
            );
        }
        for (from, to) in &self.detail.sequence_jumps {
            ui.colored_label(
                egui::Color32::from_rgb(255, 100, 100),
                format!(
                    "Warning: sequence jumped {from} -> {to} (expected {}).",
                    from.wrapping_add(1)
                ),
            );
        }

        ui.separator();

        // ---- Transaction list (newest first) ------------------------------
        ui.label(egui::RichText::new("Transactions (newest first):").strong());
        if self.detail.transactions.is_empty() {
            ui.label("  (none)");
            return;
        }

        let regions = self.detail.regions.clone();
        // Snapshot the rows we need to render so we don't borrow self.detail
        // while mutating self.selected.
        let rows: Vec<TxnRow> = self
            .detail
            .transactions
            .iter()
            .enumerate()
            .rev()
            .map(|(idx, t)| {
                let range = t.blocks.iter().fold(None::<(u64, u64)>, |acc, b| {
                    let hi = b.target + b.bsize as u64;
                    match acc {
                        None => Some((b.target, hi)),
                        Some((lo, prev_hi)) => Some((lo.min(b.target), prev_hi.max(hi))),
                    }
                });
                let range_text = match range {
                    Some((lo, hi)) => format!("{}..{}", lo / 512, hi / 512),
                    None => "-".to_string(),
                };
                TxnRow {
                    idx,
                    label: format!(
                        "#{}  {} block(s)  {}  sectors {range_text}  [{}]",
                        t.sequence_num,
                        t.blocks.len(),
                        human_bytes(t.total_bytes),
                        classification_summary(&regions, t),
                    ),
                }
            })
            .collect();

        egui::ScrollArea::vertical()
            .id_salt("journal_txns")
            .max_height(180.0)
            .show(ui, |ui| {
                for row in &rows {
                    let selected = self.selected == Some(row.idx);
                    if ui.selectable_label(selected, &row.label).clicked() {
                        self.selected = if selected { None } else { Some(row.idx) };
                    }
                }
            });

        // ---- Per-block detail of the selected transaction -----------------
        if let Some(idx) = self.selected {
            if let Some(txn) = self.detail.transactions.get(idx) {
                ui.separator();
                ui.label(
                    egui::RichText::new(format!("Transaction #{} blocks:", txn.sequence_num))
                        .strong(),
                );
                egui::ScrollArea::vertical()
                    .id_salt("journal_blocks")
                    .max_height(160.0)
                    .show(ui, |ui| {
                        for b in &txn.blocks {
                            let class = classify_target(&regions, b.target);
                            ui.label(format!(
                                "  sector {}  ({} bytes)  [{class}]  {}",
                                b.target / 512,
                                b.bsize,
                                hex_preview(&b.preview),
                            ));
                        }
                    });
            }
        }
    }
}

/// A pre-rendered transaction row in the list.
struct TxnRow {
    idx: usize,
    label: String,
}

/// One-line classification of which subsystems a transaction's blocks touch.
fn classification_summary(
    regions: &[rusty_backup::fs::hfsplus_journal::MetadataRegion],
    txn: &rusty_backup::fs::hfsplus_journal::JournalTxnDetail,
) -> String {
    let mut names: Vec<&'static str> = Vec::new();
    for b in &txn.blocks {
        let c = classify_target(regions, b.target);
        if !names.contains(&c) {
            names.push(c);
        }
    }
    if names.is_empty() {
        "empty".to_string()
    } else {
        names.join(", ")
    }
}

/// A compact hex preview of up to the first 16 bytes.
fn hex_preview(bytes: &[u8]) -> String {
    let n = bytes.len().min(16);
    let mut s = String::with_capacity(n * 3 + 2);
    for (i, b) in bytes[..n].iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(&format!("{b:02x}"));
    }
    if bytes.len() > n {
        s.push_str(" ...");
    }
    s
}

fn human_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * 1024;
    if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}
