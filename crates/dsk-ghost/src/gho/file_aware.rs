//! The file-aware directory tree walker.

use super::*;

// ---------------------------------------------------------------------------
// File-aware directory tree walker (Slice B)
// ---------------------------------------------------------------------------
//
// Algorithm (derived from PART.GHO + cross-checked against records
// 60-200 of the same fixture — see docs/gho_file_aware.md for the
// trace):
//
//   1. Track `current_dir_cluster` (initialised from the boot sector's
//      BPB_RootClus on FAT32, or sentinel 0 for FAT12/16).
//   2. When a "." dir entry is seen, set `current_dir_cluster` to that
//      entry's `first_cluster`. The writer always emits "." right after
//      descending into a directory.
//   3. When a ".." dir entry is seen, it tells us the PARENT cluster of
//      the directory we just descended into. We back-fill
//      `parent_cluster` on the most-recently-emitted DIR entry.
//   4. For a regular file (8.3, attr != dir):
//      `parent_cluster` = `current_dir_cluster`. Subsequent `0x0002` /
//      `0x0102` content records attach to it; `0x0103` finalises it.
//   5. LFN slots (attr 0x0F) buffer up before each 8.3 record; they
//      decorate the next 8.3 entry as its `long_name`.
//   6. Empty slots (first byte 0x00) and deleted slots (0xE5) are
//      source-FAT padding artifacts — ignored.
//
// **Known limitation.** The walker uses `current_dir_cluster` (last "."
// cluster) as the parent for FILE entries. If Ghost's writer ascends
// through multiple levels between a "." entry and the next 8.3, that
// implicit ascent is invisible — files in the intervening levels
// would be attributed to the deeper dir. Subdir entries are NOT
// affected (they get their true parent via their own ".." entry).

/// One entry (file or directory) recovered from a file-aware GHO.
#[derive(Debug, Clone)]
pub struct GhoFileAwareEntry {
    pub source_cluster: u32,
    pub parent_cluster: u32,
    pub short_name: String,
    pub long_name: Option<String>,
    pub attr: u8,
    pub file_size: u32,
    /// Byte offsets within the GHO of this file's `0x0002` / `0x0102`
    /// content record bodies, in stream order. Empty for directories.
    pub content_record_offsets: Vec<u64>,
    /// `0x0103` checksum value (`None` for dirs or unfinalised files).
    pub checksum: Option<u32>,
}

impl GhoFileAwareEntry {
    pub fn is_directory(&self) -> bool {
        self.attr & 0x10 != 0 && self.attr != 0x0F
    }

    /// The volume label: kept in the tree, but it is not a file.
    pub fn is_volume_label(&self) -> bool {
        self.attr & 0x08 != 0 && self.attr != 0x0F
    }

    pub fn display_name(&self) -> &str {
        self.long_name.as_deref().unwrap_or(&self.short_name)
    }
}

/// Flat tree of file-aware entries; parent/child links are implicit
/// in [`GhoFileAwareEntry::parent_cluster`].
#[derive(Debug, Clone, Default)]
pub struct GhoFileAwareTree {
    pub root_cluster: u32,
    pub entries: Vec<GhoFileAwareEntry>,
}

impl GhoFileAwareTree {
    pub fn children_of(&self, dir_cluster: u32) -> impl Iterator<Item = &GhoFileAwareEntry> {
        self.entries
            .iter()
            .filter(move |e| e.parent_cluster == dir_cluster)
    }
    pub fn file_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| !e.is_directory() && !e.is_volume_label())
            .count()
    }

    /// The root directory's volume label, which the rebuilt FAT carries as its label.
    pub fn volume_label(&self) -> Option<&str> {
        self.children_of(self.root_cluster)
            .find(|e| e.is_volume_label())
            .map(|e| e.short_name.as_str())
    }
    pub fn dir_count(&self) -> usize {
        self.entries.iter().filter(|e| e.is_directory()).count()
    }
}

pub(super) fn format_8_3_name(raw: &[u8]) -> String {
    let name = String::from_utf8_lossy(&raw[..8]).trim_end().to_string();
    let ext = String::from_utf8_lossy(&raw[8..11]).trim_end().to_string();
    if ext.is_empty() {
        name
    } else {
        format!("{}.{}", name, ext)
    }
}

pub(super) fn decode_lfn_fragment(fat_entry: &[u8; 32]) -> String {
    let mut chars: Vec<u16> = Vec::with_capacity(13);
    for &(s, e) in &[(1usize, 11usize), (14, 26), (28, 32)] {
        for chunk in fat_entry[s..e].chunks(2) {
            if chunk.len() != 2 {
                continue;
            }
            let c = u16::from_le_bytes([chunk[0], chunk[1]]);
            if c == 0 || c == 0xFFFF {
                return char::decode_utf16(chars).filter_map(|r| r.ok()).collect();
            }
            chars.push(c);
        }
    }
    char::decode_utf16(chars).filter_map(|r| r.ok()).collect()
}

/// Read a record's body, using the cache if available, otherwise
/// falling back to seeking in the file.
pub(super) fn read_record_body<R: Read + Seek>(
    reader: &mut R,
    rec: &GhoInnerRecord,
    cache: &std::collections::HashMap<u64, Vec<u8>>,
) -> Result<Vec<u8>> {
    if let Some(body) = cache.get(&rec.offset) {
        return Ok(body.clone());
    }
    reader.seek(SeekFrom::Start(rec.body_start()))?;
    let mut body = vec![0u8; rec.body_len as usize];
    reader.read_exact(&mut body)?;
    Ok(body)
}

/// Walk an already-parsed [`GhoImage`] and produce the file-aware tree.
///
/// Uses `image.cached_bodies` to avoid re-seeking for dir entry and
/// checksum record bodies when available. Falls back to `reader` for
/// any record not in the cache.
pub fn walk_file_aware_tree<R: Read + Seek>(
    reader: &mut R,
    image: &GhoImage,
) -> Result<GhoFileAwareTree> {
    let cache = &image.cached_bodies;
    let mut root_cluster: u32 = 0;
    if let Some(bs) = image
        .records
        .iter()
        .find(|r| is_boot_sector_record(r.type_code))
    {
        if bs.body_len as usize >= 48 {
            let bpb = read_record_body(reader, bs, cache)?;
            if bpb.len() >= 87 && &bpb[82..87] == b"FAT32" {
                root_cluster = u32::from_le_bytes([bpb[44], bpb[45], bpb[46], bpb[47]]);
            }
        }
    }

    let mut tree = GhoFileAwareTree {
        root_cluster,
        entries: Vec::new(),
    };
    let mut current_dir = root_cluster;
    let mut lfn_buf: Vec<String> = Vec::new();
    let mut pending_dir: Option<usize> = None;
    let mut pending_file: Option<usize> = None;

    for rec in &image.records {
        if is_dir_entry_record(rec.type_code) && rec.body_len as usize >= 36 {
            let body = read_record_body(reader, rec, cache)?;
            let entry = parse_fat_dir_entry_body(&body)?;

            if entry.is_empty_slot() || entry.is_deleted() {
                lfn_buf.clear();
                continue;
            }
            if entry.is_lfn_slot() {
                let frag = decode_lfn_fragment(&entry.fat_entry);
                if entry.fat_entry[0] & 0x40 != 0 {
                    lfn_buf.clear();
                }
                lfn_buf.insert(0, frag);
                continue;
            }

            let short = format_8_3_name(&entry.fat_entry[..11]);
            if short == "." {
                current_dir = entry.first_cluster();
                lfn_buf.clear();
                continue;
            }
            if short == ".." {
                if let Some(idx) = pending_dir.take() {
                    let parent = entry.first_cluster();
                    tree.entries[idx].parent_cluster = if parent == 0 {
                        tree.root_cluster
                    } else {
                        parent
                    };
                }
                lfn_buf.clear();
                continue;
            }

            let long_name = if !lfn_buf.is_empty() {
                Some(lfn_buf.join(""))
            } else {
                None
            };
            lfn_buf.clear();

            let is_dir = entry.is_directory();
            // A label is 11 characters with no dot, not an 8.3 name.
            let short = if entry.is_volume_label() {
                String::from_utf8_lossy(&entry.fat_entry[..11])
                    .trim_end()
                    .to_string()
            } else {
                short
            };
            tree.entries.push(GhoFileAwareEntry {
                source_cluster: entry.first_cluster(),
                parent_cluster: if is_dir { 0 } else { current_dir },
                short_name: short,
                long_name,
                attr: entry.attr_byte(),
                file_size: entry.file_size(),
                content_record_offsets: Vec::new(),
                checksum: None,
            });
            let idx = tree.entries.len() - 1;
            if is_dir {
                pending_dir = Some(idx);
                pending_file = None;
            } else if entry.is_volume_label() {
                pending_file = None;
                pending_dir = None;
            } else {
                pending_file = Some(idx);
                pending_dir = None;
            }
        } else if is_data_block_record(rec.type_code) {
            if let Some(idx) = pending_file {
                tree.entries[idx]
                    .content_record_offsets
                    .push(rec.body_start());
            }
        } else if is_checksum_record(rec.type_code) && rec.body_len as usize >= 8 {
            if let Some(idx) = pending_file.take() {
                let body = read_record_body(reader, rec, cache)?;
                tree.entries[idx].checksum = parse_checksum_record_body(&body).ok();
            }
        }
    }

    Ok(tree)
}

/// Parse a `0x0103` per-file checksum record body.
///
/// The body is `[u32 cksum][u32 cksum_dup][12 zero bytes]`. We surface
/// the checksum once and assert internally that the duplicate matches
/// (mismatch → corruption or misidentified record).
pub fn parse_checksum_record_body(body: &[u8]) -> Result<u32> {
    if body.len() < 8 {
        bail!(
            "GHO checksum record body must be >= 8 bytes, got {}",
            body.len()
        );
    }
    let a = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
    let b = u32::from_le_bytes([body[4], body[5], body[6], body[7]]);
    if a != b {
        bail!(
            "GHO checksum record has mismatched duplicate: {:#x} vs {:#x}",
            a,
            b
        );
    }
    Ok(a)
}
