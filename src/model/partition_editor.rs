//! Partition table editor state.
//!
//! Holds the working copy of partition entries the user is editing in the
//! "Edit Partition Table" popup, plus the derived list of
//! [`PartitionTableEdit`]s and validation results. The view renders text-edits
//! against these fields and calls [`PartitionEditor::build_and_validate`] on
//! the Validate / Apply buttons.
//!
//! Extracted from `gui/inspect_tab.rs` per §5 of `docs/codecleanup.md`.

use crate::partition::editor::{self as pt_editor, PartitionTableEdit};
use crate::partition::{PartitionInfo, PartitionTable};

/// Working copy of one partition entry in the editor.
#[derive(Debug, Clone)]
pub struct EditorEntry {
    pub index: usize,
    pub type_name: String,
    pub partition_type_byte: u8,
    pub partition_type_string: Option<String>,
    pub start_lba: u64,
    pub size_bytes: u64,
    pub bootable: bool,
    pub is_logical: bool,
    pub is_extended_container: bool,
    /// User-edited size text (in MiB).
    pub size_text: String,
    /// User-edited type text (hex for MBR, string for APM, GUID for GPT).
    pub type_text: String,
    /// Marked for deletion.
    pub deleted: bool,
}

impl EditorEntry {
    pub fn from_partition(p: &PartitionInfo) -> Self {
        let size_mib = p.size_bytes as f64 / (1024.0 * 1024.0);
        Self {
            index: p.index,
            type_name: p.type_name.clone(),
            partition_type_byte: p.partition_type_byte,
            partition_type_string: p.partition_type_string.clone(),
            start_lba: p.start_lba,
            size_bytes: p.size_bytes,
            bootable: p.bootable,
            is_logical: p.is_logical,
            is_extended_container: p.is_extended_container,
            size_text: format!("{:.2}", size_mib),
            type_text: if let Some(ref ts) = p.partition_type_string {
                ts.clone()
            } else {
                format!("{:02X}", p.partition_type_byte)
            },
            deleted: false,
        }
    }
}

/// Mutable state for the partition table editor popup.
#[derive(Default)]
pub struct PartitionEditor {
    /// Working copy of partition entries.
    pub entries: Vec<EditorEntry>,
    /// Edits derived from `entries` on Validate / Apply.
    pub edits: Vec<PartitionTableEdit>,
    /// Validation errors / warnings.
    pub errors: Vec<String>,
    /// Disk size in bytes for validation.
    pub disk_size: u64,
    /// Status message after validate / apply.
    pub status: Option<String>,
    /// Add-partition input fields.
    pub add_start_lba: String,
    pub add_size_mb: String,
    pub add_type: String,
    pub add_bootable: bool,
}

impl PartitionEditor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset state and seed `entries` from the current partition list.
    pub fn seed_from(&mut self, partitions: &[PartitionInfo]) {
        self.entries = partitions.iter().map(EditorEntry::from_partition).collect();
        self.edits.clear();
        self.errors.clear();
        self.status = None;
        self.add_start_lba.clear();
        self.add_size_mb.clear();
        self.add_type.clear();
        self.add_bootable = false;
        self.disk_size = partitions
            .iter()
            .map(|p| (p.start_lba * 512) + p.size_bytes)
            .max()
            .unwrap_or(0);
    }

    /// Append a new entry parsed from the add-partition input fields. Returns
    /// `true` if the inputs parsed and an entry was appended; `false` if the
    /// inputs were malformed (in which case nothing is changed).
    ///
    /// `is_mbr` controls whether the type field is interpreted as a hex byte
    /// (MBR) or an opaque string (APM / GPT).
    pub fn add_entry_from_inputs(&mut self, is_mbr: bool) -> bool {
        let (Ok(start), Ok(size_mib)) = (
            self.add_start_lba.trim().parse::<u64>(),
            self.add_size_mb.trim().parse::<f64>(),
        ) else {
            return false;
        };

        let size_bytes = (size_mib * 1024.0 * 1024.0) as u64;
        let type_byte =
            u8::from_str_radix(self.add_type.trim().trim_start_matches("0x"), 16).unwrap_or(0x83);
        let next_idx = self.entries.iter().map(|e| e.index).max().unwrap_or(0) + 1;
        let type_string = if !is_mbr {
            Some(self.add_type.trim().to_string())
        } else {
            None
        };

        self.entries.push(EditorEntry {
            index: next_idx,
            type_name: format!("0x{:02X}", type_byte),
            partition_type_byte: type_byte,
            partition_type_string: type_string,
            start_lba: start,
            size_bytes,
            bootable: self.add_bootable,
            is_logical: false,
            is_extended_container: false,
            size_text: format!("{:.2}", size_mib),
            type_text: self.add_type.trim().to_string(),
            deleted: false,
        });
        self.add_start_lba.clear();
        self.add_size_mb.clear();
        self.add_type.clear();
        self.add_bootable = false;
        true
    }

    /// Diff `entries` against `table`'s original partitions to produce the
    /// list of `PartitionTableEdit`s, then validate against the disk size.
    /// Populates `edits`, `errors`, and `status`.
    pub fn build_and_validate(&mut self, table: &PartitionTable) {
        self.edits.clear();
        self.errors.clear();
        self.status = None;

        let orig_partitions = table.partitions();

        for entry in &self.entries {
            let orig = orig_partitions.iter().find(|p| p.index == entry.index);

            if entry.deleted {
                if orig.is_some() {
                    self.edits
                        .push(PartitionTableEdit::DeleteEntry { index: entry.index });
                }
                continue;
            }

            if let Some(orig) = orig {
                let new_size_mib: f64 = entry.size_text.trim().parse().unwrap_or(-1.0);
                if new_size_mib > 0.0 {
                    let new_size_bytes = (new_size_mib * 1024.0 * 1024.0) as u64;
                    let new_size_bytes = (new_size_bytes / 512) * 512;
                    if new_size_bytes != orig.size_bytes {
                        self.edits.push(PartitionTableEdit::ResizeEntry {
                            index: entry.index,
                            new_size_bytes,
                        });
                    }
                } else {
                    self.errors.push(format!(
                        "Invalid size for partition {}: '{}'",
                        entry.index, entry.size_text
                    ));
                }

                match table {
                    PartitionTable::Mbr(_) => {
                        let new_byte =
                            u8::from_str_radix(entry.type_text.trim().trim_start_matches("0x"), 16);
                        match new_byte {
                            Ok(b) if b != orig.partition_type_byte => {
                                self.edits.push(PartitionTableEdit::ChangeType {
                                    index: entry.index,
                                    new_type_byte: b,
                                    new_type_string: None,
                                });
                            }
                            Err(_) => {
                                self.errors.push(format!(
                                    "Invalid hex type for partition {}: '{}'",
                                    entry.index, entry.type_text
                                ));
                            }
                            _ => {}
                        }
                    }
                    PartitionTable::Gpt { .. } | PartitionTable::Apm(_) => {
                        if entry.type_text.trim()
                            != orig.partition_type_string.as_deref().unwrap_or("")
                        {
                            self.edits.push(PartitionTableEdit::ChangeType {
                                index: entry.index,
                                new_type_byte: 0,
                                new_type_string: Some(entry.type_text.trim().to_string()),
                            });
                        }
                    }
                    _ => {}
                }
            } else {
                let type_byte =
                    u8::from_str_radix(entry.type_text.trim().trim_start_matches("0x"), 16)
                        .unwrap_or(0x83);
                let type_string = if entry.partition_type_string.is_some() {
                    Some(entry.type_text.trim().to_string())
                } else {
                    None
                };
                self.edits.push(PartitionTableEdit::AddEntry {
                    start_lba: entry.start_lba,
                    size_bytes: entry.size_bytes,
                    partition_type: type_byte,
                    type_string,
                    bootable: entry.bootable,
                });
            }
        }

        if self.errors.is_empty() {
            match pt_editor::validate_edits(table, &self.edits, self.disk_size) {
                Ok(warnings) => {
                    for w in warnings {
                        self.errors.push(format!("Warning: {}", w));
                    }
                    if self.edits.is_empty() {
                        self.status = Some("No changes detected.".to_string());
                    } else {
                        self.status = Some(format!(
                            "Validation OK — {} edit(s) ready to apply.",
                            self.edits.len()
                        ));
                    }
                }
                Err(e) => {
                    self.errors.push(format!("Validation error: {}", e));
                }
            }
        }
    }
}
