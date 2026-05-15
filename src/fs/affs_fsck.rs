//! AmigaDOS FFS / OFS Disk Validator.
//!
//! Four phases:
//!   1. Root block structure + checksum (verified at open time; included here
//!      so the result surfaces it cleanly).
//!   2. Walk every directory hash chain reachable from the root and verify
//!      each entry-block checksum.
//!   3. Walk file-extension chains for every file and verify pointers and
//!      block checksums.
//!   4. Rebuild the volume bitmap from observed allocations and compare it
//!      against the on-disk bitmap. A mismatch is the classic "validation
//!      needed" condition AmigaDOS surfaces when the write bit is left set
//!      after an unclean unmount.
//!
//! Repair currently handles:
//!   - `AffsBitmapMismatch` — rewrite the bitmap pages from the observed
//!     allocation set, then recompute bitmap-page checksums.
//!
//! Per-entry checksum repair is intentionally not exposed here: a stale
//! header-block checksum almost always indicates the parent directory or
//! the entry's data has been corrupted in some other way too, and silently
//! recomputing the sum would lock in that corruption. Surface it as an
//! error and let the user decide whether to wipe + restore from backup.

use std::collections::{HashSet, VecDeque};

use super::affs::AffsFilesystem;
use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, RepairReport};

/// Internal issue codes, mapped to the shared `code: String` used by the GUI.
#[derive(Debug, Clone, Copy)]
enum AffsFsckCode {
    BadChecksum,
    BitmapMismatch,
    OrphanBlock,
    BrokenHashChain,
    BadType,
    OutOfRange,
    BadDateStamp,
}

impl AffsFsckCode {
    fn as_str(&self) -> &'static str {
        match self {
            AffsFsckCode::BadChecksum => "AffsBadChecksum",
            AffsFsckCode::BitmapMismatch => "AffsBitmapMismatch",
            AffsFsckCode::OrphanBlock => "AffsOrphanBlock",
            AffsFsckCode::BrokenHashChain => "AffsBrokenHashChain",
            AffsFsckCode::BadType => "AffsBadType",
            AffsFsckCode::OutOfRange => "AffsOutOfRange",
            AffsFsckCode::BadDateStamp => "AffsBadDateStamp",
        }
    }
}

fn issue(code: AffsFsckCode, message: impl Into<String>, repairable: bool) -> FsckIssue {
    FsckIssue {
        code: code.as_str().to_string(),
        message: message.into(),
        repairable,
        debug: false,
    }
}

/// Reachable-block walk result. Used both by `check` and by `repair` so the
/// bitmap rebuild always reflects the most recent walk.
pub(crate) struct Reachable {
    /// 0/1 bit set per block index `[2 .. total_blocks)` — bit set means
    /// the block is reachable from the root.
    pub reachable: Vec<u8>,
    pub files_checked: u32,
    pub directories_checked: u32,
    pub bad_checksum_blocks: Vec<u32>,
    pub broken_chains: Vec<String>,
    pub bad_types: Vec<String>,
    pub out_of_range: Vec<String>,
}

impl Reachable {
    fn new(total_blocks: u32) -> Self {
        Self {
            reachable: vec![0u8; (total_blocks.saturating_sub(2) as usize).div_ceil(8)],
            files_checked: 0,
            directories_checked: 0,
            bad_checksum_blocks: Vec::new(),
            broken_chains: Vec::new(),
            bad_types: Vec::new(),
            out_of_range: Vec::new(),
        }
    }

    fn mark(&mut self, block: u32) {
        if block < 2 {
            return;
        }
        let bit = (block - 2) as usize;
        let byte = bit / 8;
        if byte >= self.reachable.len() {
            return;
        }
        self.reachable[byte] |= 1u8 << (bit % 8);
    }

    fn is_marked(&self, block: u32) -> bool {
        if block < 2 {
            return true;
        }
        let bit = (block - 2) as usize;
        let byte = bit / 8;
        if byte >= self.reachable.len() {
            return false;
        }
        (self.reachable[byte] >> (bit % 8)) & 1 != 0
    }
}

/// Run a non-mutating filesystem check against `fs`. The filesystem instance
/// is consumed mutably for block reads only; no on-disk writes happen.
pub fn check_affs<R: std::io::Read + std::io::Seek + Send>(
    fs: &mut AffsFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let total_blocks = fs.total_blocks();
    let mut reachable = Reachable::new(total_blocks);

    // Phase 2/3: BFS over directories starting from the root, marking
    // reachable blocks and validating checksums on the way.
    walk_directory_tree(fs, &mut reachable)?;

    for &block in &reachable.bad_checksum_blocks {
        errors.push(issue(
            AffsFsckCode::BadChecksum,
            format!("block {block}: header/data checksum mismatch"),
            false,
        ));
    }
    for msg in &reachable.broken_chains {
        errors.push(issue(AffsFsckCode::BrokenHashChain, msg.clone(), false));
    }
    for msg in &reachable.bad_types {
        errors.push(issue(AffsFsckCode::BadType, msg.clone(), false));
    }
    for msg in &reachable.out_of_range {
        errors.push(issue(AffsFsckCode::OutOfRange, msg.clone(), false));
    }

    // Phase 4: rebuild the bitmap from reachable blocks. Add the root and
    // every bitmap page itself (they're allocated but not in `reachable`).
    let observed = compute_observed_allocations(fs, &reachable);
    let stored = stored_allocation_bytes(fs, total_blocks);
    let (missing, extra) = diff_allocations(&observed, &stored, total_blocks);
    let bitmap_clean = missing.is_empty() && extra.is_empty();
    if !bitmap_clean {
        errors.push(issue(
            AffsFsckCode::BitmapMismatch,
            format!(
                "bitmap mismatch: {} block(s) marked free but allocated, \
                 {} block(s) marked allocated but unreachable",
                missing.len(),
                extra.len(),
            ),
            true,
        ));
    }

    // Phase 5 (informational): orphans — blocks the stored bitmap says are
    // allocated but the walk never reached. These typically indicate either
    // a broken hash chain or genuine orphans from a crash.
    if !extra.is_empty() {
        let preview: Vec<String> = extra.iter().take(8).map(|b| format!("{b}")).collect();
        let suffix = if extra.len() > 8 {
            format!(", ... ({} more)", extra.len() - 8)
        } else {
            String::new()
        };
        warnings.push(issue(
            AffsFsckCode::OrphanBlock,
            format!(
                "{} block(s) appear allocated but are unreachable from root: [{}{}]",
                extra.len(),
                preview.join(", "),
                suffix,
            ),
            false,
        ));
    }

    // Root block date-stamp sanity (a freshly-formatted but never-mounted
    // volume often has (0, 0, 0) — that's normal; just flag obviously bogus
    // dates as warnings).
    if let Some(root_date_warning) = root_datestamp_warning(fs) {
        warnings.push(root_date_warning);
    }

    let repairable = errors.iter().any(|e| e.repairable);
    let mut stats = FsckStats {
        files_checked: reachable.files_checked,
        directories_checked: reachable.directories_checked,
        extra: Vec::new(),
    };
    stats.extra.push((
        "Volume".to_string(),
        fs.volume_label_owned().unwrap_or_default(),
    ));
    stats.extra.push((
        "Filesystem".to_string(),
        format!("{} (variant {})", fs.fs_type_label(), fs.variant()),
    ));
    stats.extra.push((
        "Allocated blocks".to_string(),
        format!("{} / {}", fs.allocated_blocks(), total_blocks),
    ));
    if bitmap_clean {
        stats
            .extra
            .push(("Bitmap".to_string(), "consistent".to_string()));
    } else {
        stats.extra.push((
            "Bitmap".to_string(),
            format!(
                "needs rebuild ({} missing, {} orphaned)",
                missing.len(),
                extra.len()
            ),
        ));
    }
    Ok(FsckResult {
        errors,
        warnings,
        stats,
        repairable,
        orphaned_entries: Vec::new(),
    })
}

/// Repair entry point. Currently rewrites the volume bitmap from the
/// observed allocations and re-syncs. Caller is expected to be running
/// on an editable filesystem instance.
pub fn repair_affs<R: std::io::Read + std::io::Write + std::io::Seek + Send>(
    fs: &mut AffsFilesystem<R>,
) -> Result<RepairReport, FilesystemError> {
    let mut fixes_applied = Vec::new();
    let mut fixes_failed = Vec::new();
    let total_blocks = fs.total_blocks();
    let mut reachable = Reachable::new(total_blocks);
    walk_directory_tree(fs, &mut reachable)?;
    let observed = compute_observed_allocations(fs, &reachable);
    let stored = stored_allocation_bytes(fs, total_blocks);
    let (missing, extra) = diff_allocations(&observed, &stored, total_blocks);
    let unrepairable_count = reachable.bad_checksum_blocks.len()
        + reachable.broken_chains.len()
        + reachable.bad_types.len()
        + reachable.out_of_range.len();

    if missing.is_empty() && extra.is_empty() {
        return Ok(RepairReport {
            fixes_applied,
            fixes_failed,
            unrepairable_count,
        });
    }

    let total_diff = missing.len() + extra.len();
    match fs.rewrite_bitmap_from(&observed) {
        Ok(()) => {
            if let Err(e) = fs.flush_metadata() {
                fixes_failed.push(format!("bitmap rewrite flushed with error: {e}"));
            } else {
                fixes_applied.push(format!(
                    "rebuilt volume bitmap from filesystem walk ({total_diff} bit(s) corrected)",
                ));
            }
        }
        Err(e) => fixes_failed.push(format!("bitmap rewrite failed: {e}")),
    }
    Ok(RepairReport {
        fixes_applied,
        fixes_failed,
        unrepairable_count,
    })
}

fn walk_directory_tree<R: std::io::Read + std::io::Seek>(
    fs: &mut AffsFilesystem<R>,
    reach: &mut Reachable,
) -> Result<(), FilesystemError> {
    let mut queue: VecDeque<u32> = VecDeque::new();
    let mut visited: HashSet<u32> = HashSet::new();
    queue.push_back(fs.root_block_num());
    reach.mark(fs.root_block_num());
    visited.insert(fs.root_block_num());

    while let Some(dir_block) = queue.pop_front() {
        let hash_table = match fs.peek_directory_hash_table(dir_block) {
            Ok(t) => t,
            Err(e) => {
                reach
                    .bad_types
                    .push(format!("directory block {dir_block} unreadable: {e}",));
                continue;
            }
        };
        reach.directories_checked = reach.directories_checked.saturating_add(1);

        for &head in &hash_table {
            let mut next = head;
            let mut chain_len = 0u32;
            let mut chain_seen: HashSet<u32> = HashSet::new();
            while next != 0 {
                if next >= fs.total_blocks() {
                    reach.out_of_range.push(format!(
                        "hash chain in dir block {dir_block} references block {next} >= total {}",
                        fs.total_blocks()
                    ));
                    break;
                }
                if !chain_seen.insert(next) {
                    reach.broken_chains.push(format!(
                        "hash chain cycle in dir block {dir_block} at entry {next}"
                    ));
                    break;
                }
                if chain_len > 4096 {
                    reach.broken_chains.push(format!(
                        "hash chain in dir block {dir_block} exceeds 4096 entries — likely corrupt"
                    ));
                    break;
                }
                let parsed = match fs.peek_entry_block(next) {
                    Ok(p) => p,
                    Err(_) => {
                        reach.bad_checksum_blocks.push(next);
                        break;
                    }
                };
                reach.mark(next);
                match parsed.sec_type {
                    1 => {
                        // ST_ROOT — should never appear as a child.
                        reach.bad_types.push(format!(
                            "block {next}: root secType found in child position"
                        ));
                    }
                    2 => {
                        // ST_DIR — enqueue for recursion.
                        if visited.insert(parsed.block_num) {
                            queue.push_back(parsed.block_num);
                        }
                    }
                    -3 => {
                        // ST_FILE — walk data + extension chain to mark blocks.
                        reach.files_checked = reach.files_checked.saturating_add(1);
                        walk_file_extents(fs, &parsed, reach);
                    }
                    -4 | 4 | -7 => {
                        // Link entries — no extents to walk; just mark the entry.
                    }
                    other => {
                        reach
                            .bad_types
                            .push(format!("block {next}: unknown secType {other}"));
                    }
                }
                next = parsed.next_same_hash;
                chain_len += 1;
            }
        }
    }
    Ok(())
}

fn walk_file_extents<R: std::io::Read + std::io::Seek>(
    fs: &mut AffsFilesystem<R>,
    header: &super::affs::AffsEntry,
    reach: &mut Reachable,
) {
    let mut total_data_blocks_seen: u64 = 0;
    let total_blocks = fs.total_blocks() as u64;
    let mut current_data = header.data_blocks.clone();
    let mut next_ext = header.extension;
    let mut ext_seen: HashSet<u32> = HashSet::new();

    loop {
        for &dblk in &current_data {
            if dblk == 0 {
                continue;
            }
            if (dblk as u64) >= total_blocks {
                reach.out_of_range.push(format!(
                    "file {} references data block {dblk} out of range",
                    header.name
                ));
                continue;
            }
            reach.mark(dblk);
            total_data_blocks_seen += 1;
            if total_data_blocks_seen > total_blocks {
                reach.broken_chains.push(format!(
                    "file {}: data-block list exceeds volume size",
                    header.name
                ));
                return;
            }
        }
        if next_ext == 0 {
            break;
        }
        if !ext_seen.insert(next_ext) {
            reach.broken_chains.push(format!(
                "file {}: extension cycle at {next_ext}",
                header.name
            ));
            return;
        }
        if (next_ext as u64) >= total_blocks {
            reach.out_of_range.push(format!(
                "file {}: extension block {next_ext} out of range",
                header.name
            ));
            return;
        }
        match fs.peek_entry_block(next_ext) {
            Ok(ext) => {
                reach.mark(next_ext);
                current_data = ext.data_blocks;
                next_ext = ext.extension;
            }
            Err(_) => {
                reach.bad_checksum_blocks.push(next_ext);
                return;
            }
        }
    }
}

fn compute_observed_allocations<R: std::io::Read + std::io::Seek>(
    fs: &AffsFilesystem<R>,
    reach: &Reachable,
) -> Vec<u8> {
    let total = fs.total_blocks();
    // We model the bitmap as "bit set = block ALLOCATED" here for clarity;
    // the on-disk format flips this to "set = free" when we write it back.
    let bytes = (total.saturating_sub(2) as usize).div_ceil(8);
    let mut out = vec![0u8; bytes];
    for block in 2..total {
        let allocated = reach.is_marked(block) || fs.bitmap_page_owns_block(block);
        if allocated {
            let bit = (block - 2) as usize;
            out[bit / 8] |= 1u8 << (bit % 8);
        }
    }
    out
}

fn stored_allocation_bytes<R: std::io::Read + std::io::Seek>(
    fs: &AffsFilesystem<R>,
    total_blocks: u32,
) -> Vec<u8> {
    let bytes = (total_blocks.saturating_sub(2) as usize).div_ceil(8);
    let mut out = vec![0u8; bytes];
    for block in 2..total_blocks {
        if fs.block_is_allocated_public(block) {
            let bit = (block - 2) as usize;
            out[bit / 8] |= 1u8 << (bit % 8);
        }
    }
    out
}

/// Returns (`missing`, `extra`): blocks that are allocated according to the
/// walk but not the on-disk bitmap, and vice-versa.
fn diff_allocations(observed: &[u8], stored: &[u8], total_blocks: u32) -> (Vec<u32>, Vec<u32>) {
    let mut missing = Vec::new();
    let mut extra = Vec::new();
    for block in 2..total_blocks {
        let bit = (block - 2) as usize;
        let byte = bit / 8;
        let off = bit % 8;
        let obs = byte < observed.len() && (observed[byte] >> off) & 1 != 0;
        let sto = byte < stored.len() && (stored[byte] >> off) & 1 != 0;
        match (obs, sto) {
            (true, false) => missing.push(block),
            (false, true) => extra.push(block),
            _ => {}
        }
    }
    (missing, extra)
}

fn root_datestamp_warning<R: std::io::Read + std::io::Seek>(
    fs: &AffsFilesystem<R>,
) -> Option<FsckIssue> {
    let (mod_days, _, _) = fs.root_modify_datestamp();
    // Anything more than ~30 years in the future of the AmigaDOS epoch
    // (i.e. > year 2038) is almost certainly a stale stamp from a clock
    // that never got set; AmigaDOS itself stores a u32 days counter so
    // values up to ~11 million are technically valid, but in practice
    // anything past 2050 is a bug.
    if mod_days > 26_000 {
        return Some(issue(
            AffsFsckCode::BadDateStamp,
            format!("root block modify date is implausibly far in the future (days={mod_days})"),
            false,
        ));
    }
    None
}
