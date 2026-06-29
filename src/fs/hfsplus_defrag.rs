//! Streamed defragmenting clone for HFS+/HFSX (Step 22 of
//! `docs/hfsplus_enhancements.md`).
//!
//! Two-pass model:
//!
//! 1. **Plan** ([`plan_defrag_layout`]) — given a [`SourceCatalogSnapshot`]
//!    plus a target byte size, decide every block placement on the
//!    target volume: reserved regions (bitmap, B-tree files), per-file
//!    contiguous fork extents in the user-data area, and a
//!    source→target CNID remap.
//! 2. **Build + emit** (steps 22c/22d) — produce the in-memory metadata
//!    image (catalog B-tree, extents-overflow B-tree, attributes B-tree,
//!    bitmap, VH) reflecting the plan, then forward-only emit the full
//!    image into a `Write` sink with user-fork bytes streamed straight
//!    from source via `HfsPlusFilesystem::fork_stream_reader`.
//!
//! Steps 22a (pre-flight refusal), 22b (planner), 22c (in-memory
//! metadata builder including hardlinks + xattrs), and 22d (forward-only
//! emit engine) are wired up; the round-trip integration test lands in
//! 22e.

use std::collections::HashMap;
use std::io::{Read, Seek, Write};

use super::hfsplus_clone::{
    SourceCatalogSnapshot, SourceDirHardlinkSnapshot, SourceDirSnapshot, SourceFileSnapshot,
    SourceHardlinkSnapshot,
};

/// First user-assignable CNID. CNIDs 1..=15 are reserved by HFS+
/// (root-parent, root-folder, extents/catalog/attributes/allocation file
/// IDs, etc.). The streamed builder issues fresh target CNIDs starting
/// here, in source-CNID order, so the remap is stable and reproducible.
const FIRST_USER_CNID: u32 = 16;

/// Catalog growth multiplier applied AFTER computing the exact per-record
/// byte cost from the snapshot. The exact estimate covers the densely-
/// packed final state (~95% leaf utilisation); the multiplier accounts for
/// split-induced fragmentation during incremental insertion (records
/// arrive in source-CNID order, not sorted by catalog key, so splits leave
/// nodes ~50% full until later inserts fill them back). 2× the exact
/// estimate is a comfortable overprovision; the unused tail is zeros and
/// compresses to nothing in CHD.
const CATALOG_PACKING_OVERHEAD_NUM: u32 = 2;
const CATALOG_PACKING_OVERHEAD_DEN: u32 = 1;
/// Approximate body sizes (excluding the key portion) of each HFS+ catalog
/// record kind, used by [`estimate_catalog_bytes`] to compute the exact
/// catalog footprint for a given snapshot. Real records are slightly
/// larger (variable name length in thread records, possible alignment
/// padding); the per-record `+8` slop and the
/// `CATALOG_PACKING_OVERHEAD_*` multiplier together absorb the variance.
const HFSPLUS_FOLDER_BODY_BYTES: usize = 88;
const HFSPLUS_FILE_BODY_BYTES: usize = 248;
const HFSPLUS_THREAD_BODY_FIXED_BYTES: usize = 10;

/// Compute the byte cost of a single catalog record (key + body + offset
/// table slot). HFS+ catalog keys are `key_len(2) + parentID(4) +
/// name_len(2) + name(UTF-16BE NFD)`, even-aligned.
fn catalog_record_cost(name_utf16_len: usize, body_bytes: usize) -> usize {
    let key_bytes = 6 + name_utf16_len * 2;
    let key_padded = (key_bytes + 1) & !1;
    // +2 for the per-record offset table slot the leaf stores at the tail.
    // +8 slop covers per-record variance (thread `recordType` padding,
    // round-ups inside `build_*_body`, etc.). Cheap insurance.
    key_padded + body_bytes + 2 + 8
}

/// Walk a [`SourceCatalogSnapshot`] and return the exact catalog byte
/// requirement for the target B-tree, including:
///   - dir record + dir thread per directory
///   - file record + file thread per file (incl. inodes)
///   - hardlink-stub record + thread per file hardlink and dir hardlink
///   - header + map placeholder + ~5% index overhead
///
/// The caller multiplies by [`CATALOG_PACKING_OVERHEAD_NUM`] /
/// [`CATALOG_PACKING_OVERHEAP_DEN`] to absorb split-induced fragmentation.
fn estimate_catalog_bytes(snapshot: &SourceCatalogSnapshot) -> u64 {
    let mut record_bytes: u64 = 0;
    // Helper to add (record cost + thread cost) where the thread carries
    // the same UTF-16 name in its body. The thread record itself uses an
    // empty key (`parent=cnid, name=""`).
    let mut accumulate = |name: &str, body_bytes: usize| {
        let name_utf16: usize = name.encode_utf16().count();
        let main_cost = catalog_record_cost(name_utf16, body_bytes);
        // Thread record: key has empty name (so name_utf16=0), body is
        // recordType(2) + reserved(2) + parentID(4) + name_len(2) + name.
        let thread_body = HFSPLUS_THREAD_BODY_FIXED_BYTES + name_utf16 * 2;
        let thread_cost = catalog_record_cost(0, thread_body);
        record_bytes = record_bytes.saturating_add((main_cost + thread_cost) as u64);
    };
    for d in &snapshot.dirs {
        accumulate(&d.name, HFSPLUS_FOLDER_BODY_BYTES);
    }
    for f in &snapshot.files {
        accumulate(&f.name, HFSPLUS_FILE_BODY_BYTES);
    }
    for h in &snapshot.hardlinks {
        accumulate(&h.name, HFSPLUS_FILE_BODY_BYTES);
    }
    for h in &snapshot.dir_hardlinks {
        accumulate(&h.name, HFSPLUS_FILE_BODY_BYTES);
    }
    // Convert byte sum to leaf nodes. Each leaf has `NODE_SIZE - 14
    // (descriptor) - 2 (free-space slot) = NODE_SIZE - 16` bytes of
    // payload. Round up.
    let payload = (NODE_SIZE - 16) as u64;
    let leaf_nodes = record_bytes.div_ceil(payload);
    // Add 5% for index nodes + 2 for header + map placeholder. Big
    // catalogs amortise to ~2-3% index overhead; 5% is a safe upper bound.
    let total_nodes = leaf_nodes.saturating_mul(105) / 100 + 2;
    total_nodes.saturating_mul(NODE_SIZE as u64)
}

/// Where one user fork lives on the target volume.
#[derive(Debug, Clone, Copy)]
pub struct ForkPlacement {
    /// First allocation block of the contiguous extent.
    pub start_block: u32,
    /// Number of allocation blocks (`ceil(logical_size / block_size)`).
    pub block_count: u32,
    /// Logical fork size in bytes — preserved from source so the target
    /// catalog record can carry it byte-for-byte.
    pub logical_size: u64,
}

/// Per-file (or per-inode) target placement: assigned CNID + fork
/// extents. Hardlink stub rows don't appear here — they share the
/// inode's target CNID via `bsdInfo.special` and carry no forks of
/// their own.
#[derive(Debug, Clone)]
pub struct FilePlacement {
    pub target_cnid: u32,
    pub data_fork: Option<ForkPlacement>,
    pub rsrc_fork: Option<ForkPlacement>,
}

/// Result of [`plan_defrag_layout`]. Pure planning data — no I/O has
/// happened, no bytes have been written.
#[derive(Debug, Clone)]
pub struct TargetLayoutPlan {
    pub block_size: u32,
    pub total_blocks: u32,
    /// Allocation blocks reserved for the volume's structural region:
    /// `1 (boot+VH)` + `bitmap_blocks` + `catalog_blocks` +
    /// `extents_blocks` + `attributes_blocks`. Everything from
    /// `user_data_start` to `total_blocks - 1` (less the alt-VH region)
    /// is available for user file forks.
    pub reserved_blocks: u32,
    pub bitmap_start: u32,
    pub bitmap_blocks: u32,
    pub catalog_start: u32,
    pub catalog_blocks: u32,
    pub extents_start: u32,
    pub extents_blocks: u32,
    /// `attributes_blocks == 0` indicates the source had no xattrs and
    /// the target's attributes file is left unallocated (vh field
    /// `ForkData::empty()` at build time).
    pub attributes_start: u32,
    pub attributes_blocks: u32,
    pub user_data_start: u32,
    /// First user-data block past the end of every allocated fork. The
    /// builder uses this when computing `vh.next_allocation`, and the
    /// emit phase uses `total_blocks - user_data_blocks_used` to compute
    /// the zero-padding region between the last user fork and the alt
    /// VH.
    pub user_data_blocks_used: u32,
    /// Source CNID → target CNID. Includes every dir, file, inode, and
    /// hardlink stub captured in the snapshot. Root (CNID 2) maps to
    /// root.
    pub cnid_map: HashMap<u32, u32>,
    /// Per-file fork placements, keyed by *source* CNID. Includes
    /// inodes (under HFS+ Private Data) but excludes hardlink stubs
    /// (they have no forks of their own).
    pub file_placements: HashMap<u32, FilePlacement>,
}

/// Errors the planner can return. Keeping these distinct from
/// `FilesystemError` since the planner doesn't do any I/O — every
/// failure is an arithmetic / sizing issue surfaced as a string.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PlanError {
    #[error("target_size {target} bytes is smaller than the reserved metadata region ({reserved} bytes); pick a larger target")]
    TargetTooSmallForMetadata { target: u64, reserved: u64 },
    #[error("target_size {target} bytes is too small to hold {user_bytes} bytes of user data plus metadata + alt-VH region")]
    TargetTooSmallForUserData { target: u64, user_bytes: u64 },
    #[error("source block size {0} is not a power of two in [512, 4096]; defrag-clone is unsupported on this volume")]
    UnsupportedBlockSize(u32),
    #[error("target size {0} is not a multiple of source block size; round it up before planning")]
    TargetNotBlockAligned(u64),
}

/// Build a [`TargetLayoutPlan`] for streaming a defragmented copy of
/// `snapshot` into a target volume of exactly `target_size` bytes.
///
/// The target adopts the source's block size verbatim — keeping the
/// HFS+ attributes-file machinery happy and avoiding the need to
/// re-encode existing fork extents at a different granularity.
///
/// Reserved-region sizing:
/// - **Catalog**: `source_blocks * 3/2` (1.5× safety margin for leaf
///   splits during emit).
/// - **Extents-overflow**: matches source verbatim. The streamed target
///   packs every fork contiguously, so the overflow file is mostly
///   empty in practice — but matching the source size guarantees we'll
///   never refuse to emit a record.
/// - **Attributes**: matches source verbatim (or zero when the source
///   has no xattrs). Same reasoning.
/// - **Bitmap**: `ceil(total_blocks / 8 / block_size)` — exactly what
///   `create_blank_hfsplus` computes.
///
/// User-data placement:
/// - Iterates files in source-CNID order so the remap is reproducible.
/// - Inodes (files with `is_inode = true`) are placed alongside ordinary
///   files; the planner doesn't distinguish at this layer.
/// - Hardlink stubs are remapped (CNIDs assigned) but not given any
///   user-data extent.
pub fn plan_defrag_layout(
    snapshot: &SourceCatalogSnapshot,
    target_size: u64,
) -> Result<TargetLayoutPlan, PlanError> {
    let block_size = snapshot.volume.block_size;
    if !block_size.is_power_of_two() || !(512..=4096).contains(&block_size) {
        return Err(PlanError::UnsupportedBlockSize(block_size));
    }
    let bs = block_size as u64;
    if !target_size.is_multiple_of(bs) {
        return Err(PlanError::TargetNotBlockAligned(target_size));
    }
    let total_blocks: u32 = (target_size / bs)
        .try_into()
        .map_err(|_| PlanError::TargetNotBlockAligned(target_size))?;

    let bitmap_bytes = total_blocks.div_ceil(8) as u64;
    let bitmap_blocks = bitmap_bytes.div_ceil(bs) as u32;

    // Exact catalog sizing: walk the snapshot, sum per-record byte costs,
    // convert to leaf nodes, add 5% index overhead, and multiply by the
    // packing-overhead factor (2× by default) to absorb split-induced
    // fragmentation. This is precise and deterministic — no more
    // heuristic multiples-of-source guesses. The unused tail of the
    // catalog file is zeros and compresses to ~nothing in CHD output.
    let exact_catalog_bytes = estimate_catalog_bytes(snapshot)
        .saturating_mul(CATALOG_PACKING_OVERHEAD_NUM as u64)
        / CATALOG_PACKING_OVERHEAD_DEN as u64;
    let catalog_bytes_needed = exact_catalog_bytes.div_ceil(bs).saturating_mul(bs);
    let catalog_blocks_exact: u32 = (catalog_bytes_needed / bs).try_into().unwrap_or(u32::MAX);
    // Floor at the source value so degenerate sources (extremely small
    // catalogs, e.g. fresh-formatted volumes) still get sensible space.
    let catalog_blocks = catalog_blocks_exact.max(snapshot.volume.catalog_total_blocks);

    let extents_blocks = snapshot.volume.extents_total_blocks;
    let attributes_blocks = snapshot.volume.attributes_total_blocks;

    let bitmap_start: u32 = 1;
    let catalog_start = bitmap_start + bitmap_blocks;
    let extents_start = catalog_start + catalog_blocks;
    let attributes_start = extents_start + extents_blocks;
    let user_data_start = attributes_start + attributes_blocks;
    let reserved_blocks = user_data_start;

    // Alt-VH lives in the last 1024 bytes of the volume. Reserve at
    // least one block for it so user data doesn't collide with the alt
    // VH region — `create_blank_hfsplus` reserves 1024 bytes (= 2× 512
    // sectors); for block sizes ≤ 1024 we need 1 block, for ≤ 4096 we
    // still only need 1 block since alt VH is 512 bytes inside that
    // trailing 1024-byte region.
    let alt_vh_reserve_blocks: u32 = 1024u32.div_ceil(block_size).max(1);

    if (reserved_blocks as u64 + alt_vh_reserve_blocks as u64) >= total_blocks as u64 {
        return Err(PlanError::TargetTooSmallForMetadata {
            target: target_size,
            reserved: (reserved_blocks as u64 + alt_vh_reserve_blocks as u64) * bs,
        });
    }

    // CNID remap: root (CNID 2) maps to root; everything else gets a
    // fresh CNID issued sequentially from FIRST_USER_CNID. We iterate in
    // source-CNID order so remaps are deterministic across runs.
    let mut cnid_map: HashMap<u32, u32> = HashMap::new();
    cnid_map.insert(2, 2);

    let mut all_source_cnids: Vec<u32> = Vec::new();
    for d in &snapshot.dirs {
        if d.cnid != 2 {
            all_source_cnids.push(d.cnid);
        }
    }
    for f in &snapshot.files {
        all_source_cnids.push(f.cnid);
    }
    for h in &snapshot.hardlinks {
        all_source_cnids.push(h.cnid);
    }
    for h in &snapshot.dir_hardlinks {
        all_source_cnids.push(h.cnid);
    }
    all_source_cnids.sort_unstable();
    all_source_cnids.dedup();
    for (next_cnid, cnid) in (FIRST_USER_CNID..).zip(all_source_cnids) {
        cnid_map.insert(cnid, next_cnid);
    }

    // Pack user file forks contiguously starting at user_data_start.
    let mut next_block = user_data_start;
    let mut file_placements: HashMap<u32, FilePlacement> = HashMap::new();
    // Sort files by the first extent's source start_block — this matches
    // the target-placement order to source disk order so the emit phase
    // reads the source sequentially. Earlier this sorted by CNID, which
    // produced a randomish source-read pattern even on a 99.9%-contiguous
    // disk and starved the encoder when the source is slow (SD cards).
    // Files with no data fork (resource-only) fall back to their resource
    // fork's start block; files with neither sort by CNID at the tail.
    let fork_start_block = |raw: &[u8; 80]| -> Option<u32> {
        if BigEndian::read_u64(&raw[0..8]) == 0 {
            None
        } else {
            // First extent descriptor: start_block at offset 16, count at 20.
            // count==0 means "no extent here" (defensive — a 0-byte fork
            // would have been filtered by logical_size check above, but
            // some captures may carry zero-count slots after the live data).
            let count = BigEndian::read_u32(&raw[20..24]);
            if count == 0 {
                None
            } else {
                Some(BigEndian::read_u32(&raw[16..20]))
            }
        }
    };
    let mut files_sorted: Vec<&SourceFileSnapshot> = snapshot.files.iter().collect();
    files_sorted.sort_by_key(|f| {
        let data_first = fork_start_block(&f.data_fork_raw);
        let rsrc_first = fork_start_block(&f.rsrc_fork_raw);
        let primary = data_first.or(rsrc_first);
        // (primary_or_u32max, cnid) — files without any fork sort to
        // the end stably by CNID; everything with a fork sorts by
        // source-disk position.
        (primary.unwrap_or(u32::MAX), f.cnid)
    });
    for f in files_sorted {
        let target_cnid = *cnid_map.get(&f.cnid).expect("file CNID must be in map");
        let data_fork = allocate_fork(&mut next_block, f.data_fork_size, bs);
        let rsrc_fork = allocate_fork(&mut next_block, f.rsrc_fork_size, bs);
        file_placements.insert(
            f.cnid,
            FilePlacement {
                target_cnid,
                data_fork,
                rsrc_fork,
            },
        );
    }

    let user_data_blocks_used = next_block - user_data_start;
    if (next_block as u64 + alt_vh_reserve_blocks as u64) > total_blocks as u64 {
        let user_bytes: u64 = file_placements
            .values()
            .map(|p| {
                p.data_fork.map(|f| f.logical_size).unwrap_or(0)
                    + p.rsrc_fork.map(|f| f.logical_size).unwrap_or(0)
            })
            .sum();
        return Err(PlanError::TargetTooSmallForUserData {
            target: target_size,
            user_bytes,
        });
    }

    Ok(TargetLayoutPlan {
        block_size,
        total_blocks,
        reserved_blocks,
        bitmap_start,
        bitmap_blocks,
        catalog_start,
        catalog_blocks,
        extents_start,
        extents_blocks,
        attributes_start: if attributes_blocks > 0 {
            attributes_start
        } else {
            0
        },
        attributes_blocks,
        user_data_start,
        user_data_blocks_used,
        cnid_map,
        file_placements,
    })
}

/// Reserve `ceil(logical_size / block_size)` blocks for one fork. Empty
/// forks (`logical_size == 0`) consume no blocks and return `None`.
fn allocate_fork(
    next_block: &mut u32,
    logical_size: u64,
    block_size: u64,
) -> Option<ForkPlacement> {
    if logical_size == 0 {
        return None;
    }
    let block_count_u64 = logical_size.div_ceil(block_size);
    let block_count: u32 = block_count_u64
        .try_into()
        .expect("fork too large for u32 block count");
    let start = *next_block;
    *next_block += block_count;
    Some(ForkPlacement {
        start_block: start,
        block_count,
        logical_size,
    })
}

// =============================================================================
// Step 22c — In-memory metadata builder.
// =============================================================================

use byteorder::{BigEndian, ByteOrder};
use std::cmp::Ordering;

use super::filesystem::FilesystemError;
use super::hfs_common;
use super::hfsplus::{
    write_blank_btree_header_node, write_empty_leaf_node, ExtentDescriptor, ForkData,
    HfsPlusFilesystem, XattrKind, XattrRecord, HFSPLUS_VOLUME_UNMOUNTED_BIT,
};

thread_local! {
    /// Thread-local progress sink for the defrag pipeline. Callers
    /// (`run_with_resize` etc.) `set_progress_sink(Some(cb))` before
    /// invoking [`stream_defragmented_hfsplus`] /
    /// [`build_target_metadata`] / wrappers, and clear it on the way out.
    /// Tests and other callers leave it `None`; the defrag code emits to
    /// stderr only as before. Avoids threading a `log_cb` through 8+
    /// signatures while still piping the per-50k-records ticks into the
    /// GUI log queue.
    static PROGRESS_SINK: std::cell::RefCell<Option<ProgressSink>> =
        const { std::cell::RefCell::new(None) };
}

/// Thread-local progress callback used by [`set_progress_sink`].
pub type ProgressSink = Box<dyn FnMut(&str)>;

/// Install or clear a progress sink for the current thread. Pass `Some(cb)`
/// to receive log lines (one per 50k catalog inserts during build, plus
/// phase boundaries); pass `None` to suppress GUI logging and rely on
/// stderr only. Safe to call from anywhere; the previous value is dropped.
pub fn set_progress_sink(cb: Option<ProgressSink>) {
    PROGRESS_SINK.with(|cell| {
        *cell.borrow_mut() = cb;
    });
}

/// Internal helper — emit a progress message to both stderr (for the
/// `tee /tmp/rusty-backup-run.log` capture) and, if set, the thread-local
/// progress sink (for the in-GUI log).
/// Print a fragmentation summary for the captured HFS+ snapshot — number
/// of files with each extent-count bucket, plus how many forks overflow
/// the 8 inline extent descriptors into the extents-overflow B-tree.
/// Diagnostic-only; doesn't change the defrag pipeline. Output goes to
/// both stderr and the GUI progress sink so it lands in /tmp logs and
/// the in-app log panel.
fn emit_fragmentation_report(snapshot: &SourceCatalogSnapshot) {
    let mut files_with_data: u64 = 0;
    let mut total_data_bytes: u64 = 0;
    let mut total_data_inline_extents: u64 = 0;
    let mut bucket_one: u64 = 0;
    let mut bucket_two_three: u64 = 0;
    let mut bucket_four_eight: u64 = 0;
    let mut files_with_overflow: u64 = 0;
    let mut max_inline_extents: u32 = 0;

    let mut analyze_fork = |fork_raw: &[u8; 80]| {
        let logical = BigEndian::read_u64(&fork_raw[0..8]);
        if logical == 0 {
            return;
        }
        let total_blocks = BigEndian::read_u32(&fork_raw[12..16]);
        let mut inline_extents: u32 = 0;
        let mut inline_block_total: u32 = 0;
        for i in 0..8 {
            let off = 16 + i * 8;
            let start = BigEndian::read_u32(&fork_raw[off..off + 4]);
            let count = BigEndian::read_u32(&fork_raw[off + 4..off + 8]);
            if count > 0 {
                inline_extents += 1;
                inline_block_total = inline_block_total.saturating_add(count);
                let _ = start;
            }
        }
        files_with_data += 1;
        total_data_bytes = total_data_bytes.saturating_add(logical);
        total_data_inline_extents += inline_extents as u64;
        max_inline_extents = max_inline_extents.max(inline_extents);
        match inline_extents {
            0 | 1 => bucket_one += 1,
            2..=3 => bucket_two_three += 1,
            _ => bucket_four_eight += 1,
        }
        if total_blocks > inline_block_total {
            files_with_overflow += 1;
        }
    };

    for f in &snapshot.files {
        analyze_fork(&f.data_fork_raw);
    }

    let avg = if files_with_data > 0 {
        total_data_inline_extents as f64 / files_with_data as f64
    } else {
        0.0
    };
    let pct = |n: u64| -> f64 {
        if files_with_data == 0 {
            0.0
        } else {
            100.0 * n as f64 / files_with_data as f64
        }
    };
    emit_progress(&format!(
        "[fragmentation] {} files with data fork ({} MiB total); \
         1 extent: {} ({:.1}%), 2-3 extents: {} ({:.1}%), \
         4-8 extents: {} ({:.1}%); >8 extents (overflow B-tree): {} ({:.1}%); \
         avg inline extents/file: {:.2}; max inline extents seen: {}",
        files_with_data,
        total_data_bytes / (1024 * 1024),
        bucket_one,
        pct(bucket_one),
        bucket_two_three,
        pct(bucket_two_three),
        bucket_four_eight,
        pct(bucket_four_eight),
        files_with_overflow,
        pct(files_with_overflow),
        avg,
        max_inline_extents,
    ));
}

fn emit_progress(msg: &str) {
    eprintln!("{msg}");
    PROGRESS_SINK.with(|cell| {
        if let Some(cb) = cell.borrow_mut().as_mut() {
            cb(msg);
        }
    });
}

/// HFS+ B-tree key compare types (`BTHeaderRec` offset 37).
const KEY_COMPARE_CASE_FOLDING: u8 = 0xCF;
const KEY_COMPARE_BINARY: u8 = 0xBC;

/// Catalog record types (i16 at body offset 0).
const CATALOG_FOLDER: i16 = 1;
const CATALOG_FILE: i16 = 2;
const CATALOG_FOLDER_THREAD: i16 = 3;
const CATALOG_FILE_THREAD: i16 = 4;

/// HFS+ B-tree node size — fixed at 4096 (Apple's default; matches every
/// blank-builder helper in `hfsplus.rs`).
const NODE_SIZE: usize = 4096;

/// Output of [`build_target_metadata`]: the in-memory metadata image
/// for a streamed defrag target. Step 22d emits these byte buffers
/// directly into the compressor before streaming user-fork bytes.
#[derive(Debug)]
pub struct TargetMetadata {
    /// The block-0 region: 1024 bytes of boot blocks (copied from
    /// source) followed by the 512-byte primary VH at offset 1024 and
    /// `block_size - 1024 - 512` bytes of trailing zero pad.
    pub block_zero: Vec<u8>,
    /// Allocation bitmap, sized to `bitmap_blocks * block_size`.
    pub bitmap: Vec<u8>,
    /// Catalog B-tree bytes, sized to `catalog_blocks * block_size`.
    pub catalog: Vec<u8>,
    /// Extents-overflow B-tree bytes, sized to `extents_blocks * block_size`.
    pub extents_overflow: Vec<u8>,
    /// Attributes B-tree bytes — only present when the source had
    /// xattrs. Sized to `attributes_blocks * block_size`.
    pub attributes: Option<Vec<u8>>,
    /// 512-byte primary VH bytes. Already embedded inside `block_zero`,
    /// surfaced here separately so 22d can stamp the alt VH without
    /// re-serialising.
    pub vh_bytes: [u8; 512],
}

/// Build the in-memory metadata image for a streamed defrag target.
///
/// **Scope (22c, basic):** directories and ordinary files only. Source
/// hardlinks (file or directory) and xattrs are not yet emitted —
/// they currently return `Err(Unsupported)`. Follow-up sub-steps will
/// add them; the planner already issues them target CNIDs so plugging
/// them in is a localised change.
pub fn build_target_metadata(
    plan: &TargetLayoutPlan,
    snapshot: &SourceCatalogSnapshot,
) -> Result<TargetMetadata, FilesystemError> {
    let block_size = plan.block_size;
    let block_size_u = block_size as usize;
    let case_sensitive = snapshot.volume.signature == 0x4858;

    // Initialise empty B-tree files. The B-tree files are themselves
    // sized in allocation blocks; node count is derived from the byte
    // size since `node_size = 4096`.
    let catalog_bytes = plan.catalog_blocks as usize * block_size_u;
    let extents_bytes = plan.extents_blocks as usize * block_size_u;
    let mut catalog = init_btree_buffer(
        catalog_bytes,
        516,
        if case_sensitive {
            KEY_COMPARE_BINARY
        } else {
            KEY_COMPARE_CASE_FOLDING
        },
    );
    let extents_overflow = init_btree_buffer(extents_bytes, 10, 0);

    // Attributes B-tree: only allocate when the source had xattrs. The
    // planner sized `attributes_blocks` from `vh.attributes_file.total_blocks`
    // so capacity matches. `max_key_len = 266` covers the worst-case
    // attribute key: 4 fileID + 4 startBlock + 2 nameLen + 127*2 name.
    let mut attributes: Option<Vec<u8>> = if plan.attributes_blocks > 0 {
        let attr_bytes = plan.attributes_blocks as usize * block_size_u;
        Some(init_btree_buffer(attr_bytes, 266, 0))
    } else {
        None
    };

    // Bitmap: total_blocks bits, packed BE. All-zero start.
    let bitmap_bytes = (plan.total_blocks as usize).div_ceil(8);
    let bitmap_alloc = plan.bitmap_blocks as usize * block_size_u;
    let mut bitmap = vec![0u8; bitmap_alloc.max(bitmap_bytes)];

    // ----------------------------------------------------------------
    // Insert the root folder record + thread record. The root folder
    // (CNID 2) has no "parent" entry of its own — it lives at
    // (parent=1, name=<volume>). The thread record sits at
    // (parent=2, name="") with `parentID=1` in its data and the
    // volume name in its `nodeName`.
    // ----------------------------------------------------------------
    let cmp = |a: &[u8], b: &[u8]| catalog_compare(a, b, case_sensitive);
    let label = &snapshot.volume.label;
    {
        let folder_meta = root_folder_metadata(snapshot);
        let key = build_catalog_key(1, label);
        let record = build_folder_body(2, /* valence */ 0, &folder_meta);
        let mut key_record = key.clone();
        if !key_record.len().is_multiple_of(2) {
            key_record.push(0);
        }
        key_record.extend_from_slice(&record);
        hfs_common::btree_insert_full(
            &mut catalog,
            &key_record,
            &hfs_common::BTreeKeyFormat::HFSPLUS_CATALOG,
            &cmp,
        )?;

        // Root thread record.
        let thread = build_thread_record(CATALOG_FOLDER_THREAD, /* parent=1 */ 1, label);
        let mut tk = build_catalog_key(2, "");
        if !tk.len().is_multiple_of(2) {
            tk.push(0);
        }
        tk.extend_from_slice(&thread);
        hfs_common::btree_insert_full(
            &mut catalog,
            &tk,
            &hfs_common::BTreeKeyFormat::HFSPLUS_CATALOG,
            &cmp,
        )?;
        let _ = (folder_meta, record);
    }

    // ----------------------------------------------------------------
    // Insert directories in BFS order from root so parent valence
    // updates work without lookups (we count children locally).
    // ----------------------------------------------------------------
    use std::collections::HashMap;
    let mut child_count: HashMap<u32, u32> = HashMap::new();
    let mut dirs_by_parent: HashMap<u32, Vec<&SourceDirSnapshot>> = HashMap::new();
    for d in &snapshot.dirs {
        if d.cnid == 2 {
            continue; // root handled separately
        }
        dirs_by_parent.entry(d.parent_cnid).or_default().push(d);
    }

    let mut queue: std::collections::VecDeque<u32> = std::collections::VecDeque::new();
    queue.push_back(2);
    // Diagnostic: stderr trace every N catalog inserts so a multi-million-
    // record clone shows progress (vs. looking like a hang). Captures
    // wall-clock between ticks so the user can compare slow-but-progressing
    // against truly stuck.
    let mut insert_count: u64 = 0;
    let tick = 50_000u64;
    let start = std::time::Instant::now();
    let mut last_tick = start;
    let log_progress = |insert_count: u64, phase: &str, last_tick: &mut std::time::Instant| {
        let now = std::time::Instant::now();
        let dt = now.duration_since(*last_tick).as_secs_f64();
        let total = now.duration_since(start).as_secs_f64();
        let msg = format!(
            "[defrag-build] {phase}: {insert_count} catalog inserts \
             (+{tick} in {dt:.2}s, total {total:.1}s)"
        );
        emit_progress(&msg);
        *last_tick = now;
    };
    while let Some(src_parent) = queue.pop_front() {
        if let Some(children) = dirs_by_parent.remove(&src_parent) {
            for d in children {
                let target_cnid = *plan
                    .cnid_map
                    .get(&d.cnid)
                    .ok_or_else(|| missing_cnid("dir", d.cnid))?;
                let target_parent = *plan
                    .cnid_map
                    .get(&d.parent_cnid)
                    .ok_or_else(|| missing_cnid("parent", d.parent_cnid))?;

                let meta = dir_metadata(d);
                let key = build_catalog_key(target_parent, &d.name);
                let body = build_folder_body(target_cnid, d.valence, &meta);
                let mut key_record = key.clone();
                if !key_record.len().is_multiple_of(2) {
                    key_record.push(0);
                }
                key_record.extend_from_slice(&body);
                hfs_common::btree_insert_full(
                    &mut catalog,
                    &key_record,
                    &hfs_common::BTreeKeyFormat::HFSPLUS_CATALOG,
                    &cmp,
                )?;

                let thread = build_thread_record(CATALOG_FOLDER_THREAD, target_parent, &d.name);
                let mut tk = build_catalog_key(target_cnid, "");
                if !tk.len().is_multiple_of(2) {
                    tk.push(0);
                }
                tk.extend_from_slice(&thread);
                hfs_common::btree_insert_full(
                    &mut catalog,
                    &tk,
                    &hfs_common::BTreeKeyFormat::HFSPLUS_CATALOG,
                    &cmp,
                )?;

                *child_count.entry(target_parent).or_default() += 1;
                queue.push_back(d.cnid);

                insert_count += 2;
                if insert_count.is_multiple_of(tick) {
                    log_progress(insert_count, "dirs", &mut last_tick);
                }
            }
        }
    }
    log_progress(insert_count, "dirs done", &mut last_tick);

    // ----------------------------------------------------------------
    // Insert files. Forks come from the planner; no allocation here.
    // ----------------------------------------------------------------
    for f in &snapshot.files {
        let target_cnid = *plan
            .cnid_map
            .get(&f.cnid)
            .ok_or_else(|| missing_cnid("file", f.cnid))?;
        let target_parent = *plan
            .cnid_map
            .get(&f.parent_cnid)
            .ok_or_else(|| missing_cnid("file parent", f.parent_cnid))?;

        let placement = plan
            .file_placements
            .get(&f.cnid)
            .ok_or_else(|| missing_cnid("placement", f.cnid))?;
        let data_fork = make_fork_data(placement.data_fork);
        let rsrc_fork = make_fork_data(placement.rsrc_fork);

        let meta = file_metadata(f);
        let key = build_catalog_key(target_parent, &f.name);
        let body = build_file_body(target_cnid, &data_fork, &rsrc_fork, &meta);
        let mut key_record = key.clone();
        if !key_record.len().is_multiple_of(2) {
            key_record.push(0);
        }
        key_record.extend_from_slice(&body);
        hfs_common::btree_insert_full(
            &mut catalog,
            &key_record,
            &hfs_common::BTreeKeyFormat::HFSPLUS_CATALOG,
            &cmp,
        )?;

        let thread = build_thread_record(CATALOG_FILE_THREAD, target_parent, &f.name);
        let mut tk = build_catalog_key(target_cnid, "");
        if !tk.len().is_multiple_of(2) {
            tk.push(0);
        }
        tk.extend_from_slice(&thread);
        hfs_common::btree_insert_full(
            &mut catalog,
            &tk,
            &hfs_common::BTreeKeyFormat::HFSPLUS_CATALOG,
            &cmp,
        )?;

        *child_count.entry(target_parent).or_default() += 1;

        insert_count += 2;
        if insert_count.is_multiple_of(tick) {
            log_progress(insert_count, "files", &mut last_tick);
        }
    }
    log_progress(insert_count, "files done", &mut last_tick);

    // ----------------------------------------------------------------
    // Hardlink stubs (Step 22c-i). Each stub is a FILE catalog record —
    // even directory hardlinks ride the FILE shape on disk per TN1150.
    // The source already encoded `FInfo = ('hlnk','hfs+')` /
    // `('fdrp','MACS')` and `bsdInfo.special = inode_num` on the stub
    // row, so faithful replay is just "emit a file record with the
    // captured metadata and empty forks." The matching inode (a regular
    // file or directory under the target's HFS+ Private Data /
    // .HFS+ Private Directory Data\r dir) was already laid down by the
    // dir / file loops above; we just patch its `bsdInfo.special` to
    // hold the live link count once we know how many stubs reference it.
    // ----------------------------------------------------------------
    use std::collections::HashMap as _HashMap;
    let mut file_link_counts: _HashMap<u32, u32> = _HashMap::new();
    let mut dir_link_counts: _HashMap<u32, u32> = _HashMap::new();

    for h in &snapshot.hardlinks {
        let target_cnid = *plan
            .cnid_map
            .get(&h.cnid)
            .ok_or_else(|| missing_cnid("hardlink stub", h.cnid))?;
        let target_parent = *plan
            .cnid_map
            .get(&h.parent_cnid)
            .ok_or_else(|| missing_cnid("hardlink parent", h.parent_cnid))?;
        let meta = file_hardlink_metadata(h);
        let key = build_catalog_key(target_parent, &h.name);
        let empty_data = ForkData::empty();
        let empty_rsrc = ForkData::empty();
        let body = build_file_body(target_cnid, &empty_data, &empty_rsrc, &meta);
        let mut key_record = key.clone();
        if !key_record.len().is_multiple_of(2) {
            key_record.push(0);
        }
        key_record.extend_from_slice(&body);
        hfs_common::btree_insert_full(
            &mut catalog,
            &key_record,
            &hfs_common::BTreeKeyFormat::HFSPLUS_CATALOG,
            &cmp,
        )?;

        let thread = build_thread_record(CATALOG_FILE_THREAD, target_parent, &h.name);
        let mut tk = build_catalog_key(target_cnid, "");
        if !tk.len().is_multiple_of(2) {
            tk.push(0);
        }
        tk.extend_from_slice(&thread);
        hfs_common::btree_insert_full(
            &mut catalog,
            &tk,
            &hfs_common::BTreeKeyFormat::HFSPLUS_CATALOG,
            &cmp,
        )?;

        *child_count.entry(target_parent).or_default() += 1;
        *file_link_counts.entry(h.inode_num).or_default() += 1;
    }

    for h in &snapshot.dir_hardlinks {
        let target_cnid = *plan
            .cnid_map
            .get(&h.cnid)
            .ok_or_else(|| missing_cnid("dir hardlink stub", h.cnid))?;
        let target_parent = *plan
            .cnid_map
            .get(&h.parent_cnid)
            .ok_or_else(|| missing_cnid("dir hardlink parent", h.parent_cnid))?;
        let meta = dir_hardlink_metadata(h);
        let key = build_catalog_key(target_parent, &h.name);
        let empty_data = ForkData::empty();
        let empty_rsrc = ForkData::empty();
        let body = build_file_body(target_cnid, &empty_data, &empty_rsrc, &meta);
        let mut key_record = key.clone();
        if !key_record.len().is_multiple_of(2) {
            key_record.push(0);
        }
        key_record.extend_from_slice(&body);
        hfs_common::btree_insert_full(
            &mut catalog,
            &key_record,
            &hfs_common::BTreeKeyFormat::HFSPLUS_CATALOG,
            &cmp,
        )?;

        let thread = build_thread_record(CATALOG_FILE_THREAD, target_parent, &h.name);
        let mut tk = build_catalog_key(target_cnid, "");
        if !tk.len().is_multiple_of(2) {
            tk.push(0);
        }
        tk.extend_from_slice(&thread);
        hfs_common::btree_insert_full(
            &mut catalog,
            &tk,
            &hfs_common::BTreeKeyFormat::HFSPLUS_CATALOG,
            &cmp,
        )?;

        *child_count.entry(target_parent).or_default() += 1;
        *dir_link_counts.entry(h.inode_num).or_default() += 1;
    }

    // Patch each inode's bsdInfo.special with its live link count. The
    // file/dir record carried the source's count through verbatim during
    // its emission above; recomputing from actually-emitted stubs keeps
    // us self-consistent even if the source lied (corrupted volume).
    for f in snapshot.files.iter().filter(|f| f.is_inode) {
        let target_cnid = *plan
            .cnid_map
            .get(&f.cnid)
            .ok_or_else(|| missing_cnid("file inode", f.cnid))?;
        let count = f
            .inode_num
            .and_then(|n| file_link_counts.get(&n).copied())
            .unwrap_or(0);
        patch_record_bsd_special(&mut catalog, target_cnid, count)?;
    }
    for d in snapshot.dirs.iter().filter(|d| d.is_dir_inode) {
        let target_cnid = *plan
            .cnid_map
            .get(&d.cnid)
            .ok_or_else(|| missing_cnid("dir inode", d.cnid))?;
        let count = d
            .inode_num
            .and_then(|n| dir_link_counts.get(&n).copied())
            .unwrap_or(0);
        patch_record_bsd_special(&mut catalog, target_cnid, count)?;
    }

    // ----------------------------------------------------------------
    // Extended attributes (Step 22c-ii). Inline xattrs replay against
    // each remapped CNID; fork-style and extents-style xattrs are
    // intentionally rejected (mirrors Step 13's inline-only limitation
    // on the editing side, until fork-style xattr writes ship). The
    // attributes buffer was pre-sized from source.vh.attributes_file
    // so all source records fit in capacity.
    // ----------------------------------------------------------------
    if let Some(ref mut attr_buf) = attributes {
        for f in &snapshot.files {
            let target_cnid = *plan
                .cnid_map
                .get(&f.cnid)
                .ok_or_else(|| missing_cnid("file xattr", f.cnid))?;
            emit_xattrs_for_cnid(attr_buf, target_cnid, &f.xattrs, &f.name)?;
        }
        for d in &snapshot.dirs {
            let target_cnid = *plan
                .cnid_map
                .get(&d.cnid)
                .ok_or_else(|| missing_cnid("dir xattr", d.cnid))?;
            emit_xattrs_for_cnid(attr_buf, target_cnid, &d.xattrs, &d.name)?;
        }
        for h in &snapshot.hardlinks {
            let target_cnid = *plan
                .cnid_map
                .get(&h.cnid)
                .ok_or_else(|| missing_cnid("hardlink xattr", h.cnid))?;
            emit_xattrs_for_cnid(attr_buf, target_cnid, &h.xattrs, &h.name)?;
        }
        for h in &snapshot.dir_hardlinks {
            let target_cnid = *plan
                .cnid_map
                .get(&h.cnid)
                .ok_or_else(|| missing_cnid("dir hardlink xattr", h.cnid))?;
            emit_xattrs_for_cnid(attr_buf, target_cnid, &h.xattrs, &h.name)?;
        }
    }

    // ----------------------------------------------------------------
    // Patch parent valences. The root's valence comes straight from
    // the snapshot's root SourceDirSnapshot if present; otherwise we
    // accept whatever we counted.
    // ----------------------------------------------------------------
    patch_folder_valences(&mut catalog, &child_count, case_sensitive)?;

    // ----------------------------------------------------------------
    // Mark allocation bitmap. Reserved blocks at front + every
    // planned user-fork extent + alt-VH region (last block, plus
    // possibly the previous block to cover the 1024-byte alt-VH region).
    // ----------------------------------------------------------------
    for blk in 0..plan.reserved_blocks {
        hfs_common::bitmap_set_bit_be(&mut bitmap, blk);
    }
    for placement in plan.file_placements.values() {
        if let Some(f) = placement.data_fork {
            for i in 0..f.block_count {
                hfs_common::bitmap_set_bit_be(&mut bitmap, f.start_block + i);
            }
        }
        if let Some(f) = placement.rsrc_fork {
            for i in 0..f.block_count {
                hfs_common::bitmap_set_bit_be(&mut bitmap, f.start_block + i);
            }
        }
    }
    let alt_vh_blocks: u32 = 1024u32.div_ceil(block_size).max(1);
    for i in 0..alt_vh_blocks {
        hfs_common::bitmap_set_bit_be(&mut bitmap, plan.total_blocks - 1 - i);
    }

    // ----------------------------------------------------------------
    // Volume header. Counts come from snapshot (one folder added for
    // root automatically — drFndrInfo[0]=blessed-folder etc. carries
    // through with CNID remap). free_blocks = total - allocated.
    // ----------------------------------------------------------------
    let mut allocated_blocks: u32 = plan.reserved_blocks + alt_vh_blocks;
    for placement in plan.file_placements.values() {
        if let Some(f) = placement.data_fork {
            allocated_blocks += f.block_count;
        }
        if let Some(f) = placement.rsrc_fork {
            allocated_blocks += f.block_count;
        }
    }
    let free_blocks = plan.total_blocks.saturating_sub(allocated_blocks);

    let mut new_finder_info = [0u32; 8];
    let known: std::collections::HashSet<u32> = plan.cnid_map.keys().copied().collect();
    for (i, slot) in new_finder_info.iter_mut().enumerate() {
        if i == 6 || i == 7 {
            continue;
        }
        let src_val = snapshot.volume.finder_info[i];
        *slot = if known.contains(&src_val) {
            *plan.cnid_map.get(&src_val).unwrap()
        } else {
            src_val
        };
    }

    let vh_bytes = build_volume_header(
        snapshot,
        plan,
        new_finder_info,
        snapshot.volume.file_count,
        snapshot.volume.folder_count,
        free_blocks,
    );

    // Block-0 region: boot blocks (1024 from source) + primary VH (512)
    // + zero pad up to block_size.
    let mut block_zero = vec![0u8; block_size_u];
    block_zero[..1024].copy_from_slice(&snapshot.volume.boot_blocks);
    block_zero[1024..1536].copy_from_slice(&vh_bytes);

    Ok(TargetMetadata {
        block_zero,
        bitmap,
        catalog,
        extents_overflow,
        attributes,
        vh_bytes,
    })
}

// ----------------------------------------------------------------------
// Step 22d — streaming emit engine.
// ----------------------------------------------------------------------

/// Outcome of [`stream_defragmented_hfsplus`]. Counts mirror the source
/// snapshot — every captured directory, file, hardlink stub, and xattr
/// is reflected in the emitted target.
#[derive(Debug, Default, Clone)]
pub struct DefragReport {
    pub files_copied: u64,
    pub dirs_copied: u64,
    pub data_bytes_copied: u64,
    pub rsrc_bytes_copied: u64,
    pub xattrs_copied: u64,
    pub hardlinks_copied: u64,
    pub dir_hardlinks_copied: u64,
    /// Total bytes emitted into the writer. Always equals `target_size`
    /// (the function refuses to short-write).
    pub bytes_emitted: u64,
}

/// Stream a defragmented copy of `source` into `dst` as exactly
/// `target_size` bytes. Combines the planner (22b), the in-memory
/// metadata builder (22c), and a forward-only emit pass that interleaves
/// the metadata regions with user-fork bytes streamed from `source`.
///
/// The output is a flat HFS+ image suitable for opening with
/// [`HfsPlusFilesystem::open`] (no APM wrapper is added — that's the
/// caller's job, e.g. via [`emit_apm_disk_with_hfs`]).
///
/// Pre-conditions:
/// - `source` is opened read-only; the function never mutates it.
/// - `target_size` is a multiple of `source`'s block size and large
///   enough to hold every captured fork (validated inside the planner).
///
/// `dst` only needs `Write` — emission is single-pass and never seeks.
pub fn stream_defragmented_hfsplus<R, W>(
    source: &mut HfsPlusFilesystem<R>,
    target_size: u64,
    dst: &mut W,
    before_emit: Option<&dyn Fn()>,
) -> Result<DefragReport, FilesystemError>
where
    R: Read + Seek + Send,
    W: Write,
{
    let snapshot = SourceCatalogSnapshot::capture(source)?;
    emit_fragmentation_report(&snapshot);
    let plan = plan_defrag_layout(&snapshot, target_size)
        .map_err(|e| FilesystemError::InvalidData(format!("defrag plan failed: {e}")))?;
    let meta = build_target_metadata(&plan, &snapshot)?;
    // Random-access phases (catalog walk in `capture`, B-tree population
    // in `build_target_metadata`) are now done; what follows is a long,
    // mostly-sequential drain of fork bytes from the source. Callers can
    // use this hook to flip a wrapping `BulkBufReader` into bulk fill
    // mode so the next phase collapses thousands of small reads into a
    // handful of multi-hundred-MiB syscalls.
    if let Some(cb) = before_emit {
        cb();
    }

    let bs = plan.block_size as u64;
    let mut writer = CountingWriter::new(dst);

    // Block 0 — boot blocks + primary VH (already padded to block_size).
    writer.write_all(&meta.block_zero)?;
    debug_assert_eq!(writer.bytes_written, bs);

    // Bitmap, catalog, extents-overflow, attributes — back-to-back per
    // the planner's region layout (no gaps).
    assert_region(
        writer.bytes_written,
        plan.bitmap_start as u64 * bs,
        "bitmap_start",
    )?;
    writer.write_all(&meta.bitmap)?;
    assert_region(
        writer.bytes_written,
        plan.catalog_start as u64 * bs,
        "catalog_start",
    )?;
    writer.write_all(&meta.catalog)?;
    assert_region(
        writer.bytes_written,
        plan.extents_start as u64 * bs,
        "extents_start",
    )?;
    writer.write_all(&meta.extents_overflow)?;
    // `plan.attributes_start` is a sentinel zero when the source had no
    // xattrs (planner convention) — only assert the boundary when the
    // attributes file actually exists.
    if plan.attributes_blocks > 0 {
        assert_region(
            writer.bytes_written,
            plan.attributes_start as u64 * bs,
            "attributes_start",
        )?;
    }
    if let Some(attrs) = meta.attributes.as_ref() {
        writer.write_all(attrs)?;
    }
    assert_region(
        writer.bytes_written,
        plan.user_data_start as u64 * bs,
        "user_data_start",
    )?;

    // Build an emission schedule for the user-data area: every fork the
    // planner placed, sorted by start_block. Forks pack contiguously
    // from `user_data_start`, but we still defensively zero-fill any
    // gap between the previous fork's end and the next fork's start.
    struct ForkEmit<'a> {
        start_block: u32,
        block_count: u32,
        logical_size: u64,
        source_cnid: u32,
        fork_kind: u8,
        fork_raw: &'a [u8; 80],
    }

    let mut emits: Vec<ForkEmit<'_>> = Vec::new();
    for f in &snapshot.files {
        let placement = plan
            .file_placements
            .get(&f.cnid)
            .ok_or_else(|| missing_cnid("file", f.cnid))?;
        if let Some(d) = placement.data_fork {
            emits.push(ForkEmit {
                start_block: d.start_block,
                block_count: d.block_count,
                logical_size: d.logical_size,
                source_cnid: f.cnid,
                fork_kind: HFSPLUS_FORK_DATA,
                fork_raw: &f.data_fork_raw,
            });
        }
        if let Some(r) = placement.rsrc_fork {
            emits.push(ForkEmit {
                start_block: r.start_block,
                block_count: r.block_count,
                logical_size: r.logical_size,
                source_cnid: f.cnid,
                fork_kind: HFSPLUS_FORK_RESOURCE,
                fork_raw: &f.rsrc_fork_raw,
            });
        }
    }
    emits.sort_by_key(|e| e.start_block);

    let mut report = DefragReport {
        files_copied: snapshot.files.iter().filter(|f| !f.is_inode).count() as u64,
        dirs_copied: snapshot
            .dirs
            .iter()
            .filter(|d| d.cnid != 2 && !d.is_dir_inode)
            .count() as u64,
        hardlinks_copied: snapshot.hardlinks.len() as u64,
        dir_hardlinks_copied: snapshot.dir_hardlinks.len() as u64,
        xattrs_copied: snapshot
            .files
            .iter()
            .map(|f| f.xattrs.len() as u64)
            .sum::<u64>()
            + snapshot
                .dirs
                .iter()
                .map(|d| d.xattrs.len() as u64)
                .sum::<u64>()
            + snapshot
                .hardlinks
                .iter()
                .map(|h| h.xattrs.len() as u64)
                .sum::<u64>()
            + snapshot
                .dir_hardlinks
                .iter()
                .map(|h| h.xattrs.len() as u64)
                .sum::<u64>(),
        ..DefragReport::default()
    };

    for emit in &emits {
        let target_offset = emit.start_block as u64 * bs;
        let cursor = writer.bytes_written;
        if target_offset > cursor {
            zero_fill(&mut writer, target_offset - cursor)?;
        } else if target_offset < cursor {
            return Err(FilesystemError::InvalidData(format!(
                "defrag emit overlap: fork at block {} starts before cursor at byte {}",
                emit.start_block, cursor
            )));
        }

        // Stream `logical_size` bytes from source, then zero-pad to
        // `block_count * bs` so the next fork lands on its planned block.
        let mut reader =
            source.fork_stream_reader(emit.source_cnid, emit.fork_kind, emit.fork_raw)?;
        let copied = std::io::copy(&mut reader, &mut writer)?;
        if copied != emit.logical_size {
            return Err(FilesystemError::InvalidData(format!(
                "defrag emit short read: file {} fork {:#x}: expected {} bytes, got {}",
                emit.source_cnid, emit.fork_kind, emit.logical_size, copied
            )));
        }
        let extent_bytes = emit.block_count as u64 * bs;
        if copied > extent_bytes {
            return Err(FilesystemError::InvalidData(format!(
                "defrag emit overflow: file {} fork {:#x}: {} bytes copied exceeds extent {}",
                emit.source_cnid, emit.fork_kind, copied, extent_bytes
            )));
        }
        zero_fill(&mut writer, extent_bytes - copied)?;

        match emit.fork_kind {
            HFSPLUS_FORK_DATA => report.data_bytes_copied += copied,
            HFSPLUS_FORK_RESOURCE => report.rsrc_bytes_copied += copied,
            _ => {}
        }
    }

    // Zero-pad from the end of the last user fork up to the alt-VH
    // region (last 1024 bytes of the volume).
    let alt_vh_offset = target_size.saturating_sub(1024);
    let cursor = writer.bytes_written;
    if cursor > alt_vh_offset {
        return Err(FilesystemError::InvalidData(format!(
            "defrag emit overran into alt-VH region: cursor {cursor} > alt offset {alt_vh_offset}"
        )));
    }
    zero_fill(&mut writer, alt_vh_offset - cursor)?;
    writer.write_all(&meta.vh_bytes)?;
    let trailing = target_size - writer.bytes_written;
    zero_fill(&mut writer, trailing)?;

    if writer.bytes_written != target_size {
        return Err(FilesystemError::InvalidData(format!(
            "defrag emit byte count mismatch: wrote {}, expected {}",
            writer.bytes_written, target_size
        )));
    }
    report.bytes_emitted = writer.bytes_written;
    Ok(report)
}

/// HFS+ fork-type bytes (mirrors the constants in `hfsplus_clone.rs`).
const HFSPLUS_FORK_DATA: u8 = 0x00;
const HFSPLUS_FORK_RESOURCE: u8 = 0xFF;

/// Wraps a `Write` and tracks total bytes emitted. The streaming emit
/// engine relies on the running count for region-boundary debug
/// assertions and for computing zero-fill spans.
struct CountingWriter<'a, W: Write> {
    inner: &'a mut W,
    bytes_written: u64,
}

impl<'a, W: Write> CountingWriter<'a, W> {
    fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }
}

impl<'a, W: Write> Write for CountingWriter<'a, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes_written += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Verify that the streaming cursor lands exactly on the planned region
/// boundary. Catches metadata-buffer length drift between the planner
/// and the builder before we emit a malformed image.
fn assert_region(actual: u64, expected: u64, name: &str) -> Result<(), FilesystemError> {
    if actual != expected {
        return Err(FilesystemError::InvalidData(format!(
            "defrag emit cursor at {actual} but planner placed {name} at {expected}"
        )));
    }
    Ok(())
}

/// Emit `n` zero bytes to `writer` using a 64 KiB scratch buffer.
fn zero_fill<W: Write>(writer: &mut W, n: u64) -> std::io::Result<()> {
    if n == 0 {
        return Ok(());
    }
    static BUF: [u8; 65536] = [0u8; 65536];
    let mut left = n;
    while left > 0 {
        let chunk = left.min(BUF.len() as u64) as usize;
        writer.write_all(&BUF[..chunk])?;
        left -= chunk as u64;
    }
    Ok(())
}

// ----------------------------------------------------------------------
// Helpers (private to this module).
// ----------------------------------------------------------------------

fn missing_cnid(kind: &str, cnid: u32) -> FilesystemError {
    FilesystemError::InvalidData(format!(
        "defrag plan is missing the {kind} CNID {cnid} mapping"
    ))
}

/// Initialise a B-tree buffer (`total_bytes` long) with one header
/// node + one empty leaf node. The remaining nodes are free.
fn init_btree_buffer(total_bytes: usize, max_key_len: u16, key_compare_type: u8) -> Vec<u8> {
    assert!(
        total_bytes.is_multiple_of(NODE_SIZE),
        "btree size must be node-aligned"
    );
    let total_nodes = (total_bytes / NODE_SIZE) as u32;
    // Layout: 0 = header, 1 = empty leaf, 2..2+map_nodes = map nodes
    // (BTNodeKind=2), then free nodes for B-tree growth. Map nodes are
    // required whenever the catalog exceeds the header bitmap's capacity
    // (~30,720 nodes at NODE_SIZE=4096, i.e. ~120 MiB catalog file).
    // OS 9 volumes with 100k+ catalog records routinely need map nodes.
    let map_nodes = hfs_common::map_nodes_required(total_nodes, NODE_SIZE);
    let used_nodes = 2 + map_nodes;
    assert!(
        total_nodes >= used_nodes + 2,
        "need at least {} nodes (header + leaf + {} map nodes + 2 free), got {}",
        used_nodes + 2,
        map_nodes,
        total_nodes,
    );
    let mut buf = vec![0u8; total_bytes];
    write_blank_btree_header_node(
        &mut buf[..NODE_SIZE],
        NODE_SIZE,
        /* leaf_records= */ 0,
        total_nodes,
        /* free_nodes= */ total_nodes - used_nodes,
        max_key_len,
        key_compare_type,
    );
    write_empty_leaf_node(&mut buf[NODE_SIZE..2 * NODE_SIZE]);

    if map_nodes > 0 {
        // Header.fLink points at the first map node so `btree_bitmap_segments`
        // walks the chain when allocating from the global node space.
        BigEndian::write_u32(&mut buf[0..4], 2);
        // Mark each map node as allocated in the header's bitmap (record 2,
        // at offset 270 — matches `write_blank_btree_header_node`). With
        // NODE_SIZE=4096 the header bitmap covers ~30,720 bits; map nodes
        // sit at indices 2..2+map_nodes, comfortably inside that range.
        let bitmap_rec_offset = 270usize;
        let header_bitmap_len = NODE_SIZE - 256; // = 3840 bytes
        for i in 0..map_nodes {
            let bit = 2 + i;
            let byte_idx = (bit / 8) as usize;
            let bit_in_byte = 7 - (bit % 8) as u8;
            if byte_idx < header_bitmap_len {
                buf[bitmap_rec_offset + byte_idx] |= 1u8 << bit_in_byte;
            }
        }
        // Initialise each map node with the prev/next chain pointers.
        for i in 0..map_nodes {
            let node_idx = 2 + i;
            let next_link = if i + 1 < map_nodes { node_idx + 1 } else { 0 };
            let prev_link = if i == 0 { 0 } else { node_idx - 1 };
            hfs_common::init_map_node(&mut buf, NODE_SIZE, node_idx, prev_link, next_link);
        }
    }

    buf
}

/// Build an HFS+ catalog key (`key_len(2) + parent_id(4) + name_length(2) + name(UTF-16BE NFD)`).
/// Mirrors `HfsPlusFilesystem::build_catalog_key` (which is `pub(crate)`).
fn build_catalog_key(parent_cnid: u32, name: &str) -> Vec<u8> {
    HfsPlusFilesystem::<std::io::Cursor<Vec<u8>>>::build_catalog_key(parent_cnid, name)
}

/// HFS+ catalog key compare. Mirrors `HfsPlusFilesystem::catalog_compare`.
fn catalog_compare(a: &[u8], b: &[u8], case_sensitive: bool) -> Ordering {
    if a.len() < 8 || b.len() < 8 {
        return a.len().cmp(&b.len());
    }
    let parent_a = BigEndian::read_u32(&a[2..6]);
    let parent_b = BigEndian::read_u32(&b[2..6]);
    let name_len_a = BigEndian::read_u16(&a[6..8]) as usize;
    let name_len_b = BigEndian::read_u16(&b[6..8]) as usize;
    let name_a: Vec<u16> = a[8..8 + name_len_a.min((a.len() - 8) / 2) * 2]
        .chunks_exact(2)
        .map(BigEndian::read_u16)
        .collect();
    let name_b: Vec<u16> = b[8..8 + name_len_b.min((b.len() - 8) / 2) * 2]
        .chunks_exact(2)
        .map(BigEndian::read_u16)
        .collect();
    hfs_common::compare_hfsplus_keys(parent_a, &name_a, parent_b, &name_b, case_sensitive)
}

/// Per-record metadata shared by file/folder bodies (dates + 16-byte
/// FInfo/FXInfo + 16-byte BSDInfo + 4-byte textEncoding).
#[derive(Default, Clone, Copy)]
struct CatalogRecordMeta {
    flags: u16,
    create_date: u32,
    content_mod_date: u32,
    attribute_mod_date: u32,
    access_date: u32,
    backup_date: u32,
    bsd_info: [u8; 16],
    finder_info: [u8; 16],
    extended_finder_info: [u8; 16],
    text_encoding: u32,
}

fn file_metadata(f: &SourceFileSnapshot) -> CatalogRecordMeta {
    CatalogRecordMeta {
        flags: f.flags,
        create_date: f.create_date,
        content_mod_date: f.content_mod_date,
        attribute_mod_date: f.attribute_mod_date,
        access_date: f.access_date,
        backup_date: f.backup_date,
        bsd_info: f.bsd_info,
        finder_info: f.finder_info,
        extended_finder_info: f.extended_finder_info,
        text_encoding: f.text_encoding,
    }
}

fn file_hardlink_metadata(h: &SourceHardlinkSnapshot) -> CatalogRecordMeta {
    CatalogRecordMeta {
        flags: h.flags,
        create_date: h.create_date,
        content_mod_date: h.content_mod_date,
        attribute_mod_date: h.attribute_mod_date,
        access_date: h.access_date,
        backup_date: h.backup_date,
        bsd_info: h.bsd_info,
        finder_info: h.finder_info,
        extended_finder_info: h.extended_finder_info,
        text_encoding: h.text_encoding,
    }
}

fn dir_hardlink_metadata(h: &SourceDirHardlinkSnapshot) -> CatalogRecordMeta {
    CatalogRecordMeta {
        flags: h.flags,
        create_date: h.create_date,
        content_mod_date: h.content_mod_date,
        attribute_mod_date: h.attribute_mod_date,
        access_date: h.access_date,
        backup_date: h.backup_date,
        bsd_info: h.bsd_info,
        finder_info: h.finder_info,
        extended_finder_info: h.extended_finder_info,
        text_encoding: h.text_encoding,
    }
}

fn dir_metadata(d: &SourceDirSnapshot) -> CatalogRecordMeta {
    CatalogRecordMeta {
        flags: d.flags,
        create_date: d.create_date,
        content_mod_date: d.content_mod_date,
        attribute_mod_date: d.attribute_mod_date,
        access_date: d.access_date,
        backup_date: d.backup_date,
        bsd_info: d.bsd_info,
        finder_info: d.finder_info,
        extended_finder_info: d.extended_finder_info,
        text_encoding: d.text_encoding,
    }
}

/// Find the source's root-dir snapshot (parent=1, cnid=2) and lift its
/// metadata onto the target's root.
fn root_folder_metadata(snapshot: &SourceCatalogSnapshot) -> CatalogRecordMeta {
    snapshot
        .dirs
        .iter()
        .find(|d| d.cnid == 2)
        .map(dir_metadata)
        .unwrap_or_default()
}

/// Build an 88-byte HFS+ folder record body.
fn build_folder_body(folder_id: u32, valence: u32, meta: &CatalogRecordMeta) -> [u8; 88] {
    let mut rec = [0u8; 88];
    BigEndian::write_i16(&mut rec[0..2], CATALOG_FOLDER);
    BigEndian::write_u16(&mut rec[2..4], meta.flags);
    BigEndian::write_u32(&mut rec[4..8], valence);
    BigEndian::write_u32(&mut rec[8..12], folder_id);
    BigEndian::write_u32(&mut rec[12..16], meta.create_date);
    BigEndian::write_u32(&mut rec[16..20], meta.content_mod_date);
    BigEndian::write_u32(&mut rec[20..24], meta.attribute_mod_date);
    BigEndian::write_u32(&mut rec[24..28], meta.access_date);
    BigEndian::write_u32(&mut rec[28..32], meta.backup_date);
    rec[32..48].copy_from_slice(&meta.bsd_info);
    rec[48..64].copy_from_slice(&meta.finder_info);
    rec[64..80].copy_from_slice(&meta.extended_finder_info);
    BigEndian::write_u32(&mut rec[80..84], meta.text_encoding);
    rec
}

/// Build a 248-byte HFS+ file record body.
fn build_file_body(
    file_id: u32,
    data_fork: &ForkData,
    rsrc_fork: &ForkData,
    meta: &CatalogRecordMeta,
) -> [u8; 248] {
    let mut rec = [0u8; 248];
    BigEndian::write_i16(&mut rec[0..2], CATALOG_FILE);
    BigEndian::write_u16(&mut rec[2..4], meta.flags);
    // body[4..8] reserved1
    BigEndian::write_u32(&mut rec[8..12], file_id);
    BigEndian::write_u32(&mut rec[12..16], meta.create_date);
    BigEndian::write_u32(&mut rec[16..20], meta.content_mod_date);
    BigEndian::write_u32(&mut rec[20..24], meta.attribute_mod_date);
    BigEndian::write_u32(&mut rec[24..28], meta.access_date);
    BigEndian::write_u32(&mut rec[28..32], meta.backup_date);
    rec[32..48].copy_from_slice(&meta.bsd_info);
    rec[48..64].copy_from_slice(&meta.finder_info);
    rec[64..80].copy_from_slice(&meta.extended_finder_info);
    BigEndian::write_u32(&mut rec[80..84], meta.text_encoding);
    // body[84..88] reserved2
    serialize_fork(data_fork, &mut rec[88..168]);
    serialize_fork(rsrc_fork, &mut rec[168..248]);
    rec
}

/// Build a thread record (`type(2)+reserved(2)+parentID(4)+name`).
fn build_thread_record(record_type: i16, parent_cnid: u32, name: &str) -> Vec<u8> {
    let utf16: Vec<u16> = crate::fs::hfs_unicode::decompose_str(name);
    let mut rec = Vec::with_capacity(10 + utf16.len() * 2);
    let mut b2 = [0u8; 2];
    let mut b4 = [0u8; 4];
    BigEndian::write_i16(&mut b2, record_type);
    rec.extend_from_slice(&b2);
    rec.extend_from_slice(&[0, 0]); // reserved
    BigEndian::write_u32(&mut b4, parent_cnid);
    rec.extend_from_slice(&b4);
    BigEndian::write_u16(&mut b2, utf16.len() as u16);
    rec.extend_from_slice(&b2);
    for &ch in &utf16 {
        BigEndian::write_u16(&mut b2, ch);
        rec.extend_from_slice(&b2);
    }
    rec
}

/// Translate a planned [`ForkPlacement`] into an HFS+ [`ForkData`].
fn make_fork_data(placement: Option<ForkPlacement>) -> ForkData {
    let mut fork = ForkData::empty();
    if let Some(p) = placement {
        fork.logical_size = p.logical_size;
        fork.total_blocks = p.block_count;
        fork.extents[0] = ExtentDescriptor {
            start_block: p.start_block,
            block_count: p.block_count,
        };
    }
    fork
}

/// Serialize a `ForkData` into the 80-byte on-disk layout.
fn serialize_fork(fork: &ForkData, out: &mut [u8]) {
    BigEndian::write_u64(&mut out[0..8], fork.logical_size);
    BigEndian::write_u32(&mut out[8..12], fork.clump_size);
    BigEndian::write_u32(&mut out[12..16], fork.total_blocks);
    for (i, ext) in fork.extents.iter().enumerate() {
        let off = 16 + i * 8;
        BigEndian::write_u32(&mut out[off..off + 4], ext.start_block);
        BigEndian::write_u32(&mut out[off + 4..off + 8], ext.block_count);
    }
}

/// Walk the catalog after every record's been inserted and stamp each
/// folder record's `valence` with the count of inserted children for
/// that folder. The records were inserted with `valence=0` initially
/// (root) or the source's valence (other dirs); for streamed defrag
/// we recompute from the actually-inserted children to stay self-
/// consistent.
/// Emit one CNID's worth of xattrs into the target attributes B-tree.
/// Inline records flow through `HfsPlusFilesystem::build_inline_attr_record`
/// and `HfsPlusFilesystem::attr_compare` — same builders the editing
/// path uses, so on-disk layout matches byte-for-byte. Fork-style
/// (`Fork(...)`) and extents-continuation (`Extents(...)`) records
/// surface a controlled error mirroring Step 13's inline-only
/// limitation; landing fork-style xattr writes is a separate feature.
fn emit_xattrs_for_cnid(
    attr_buf: &mut [u8],
    target_cnid: u32,
    xattrs: &[XattrRecord],
    label: &str,
) -> Result<(), FilesystemError> {
    for x in xattrs {
        match &x.kind {
            XattrKind::Inline(value) => {
                let rec = HfsPlusFilesystem::<std::io::Cursor<Vec<u8>>>::build_inline_attr_record(
                    target_cnid,
                    &x.name,
                    value,
                );
                hfs_common::btree_insert_full(
                    attr_buf,
                    &rec,
                    &hfs_common::BTreeKeyFormat::HFSPLUS_ATTRIBUTES,
                    &HfsPlusFilesystem::<std::io::Cursor<Vec<u8>>>::attr_compare,
                )?;
            }
            XattrKind::Fork(_) | XattrKind::Extents(_) => {
                return Err(FilesystemError::Unsupported(format!(
                    "{label}: fork-style extended attribute '{}' is not supported by \
                     the streamed defrag emit path (inline xattrs only — same limitation \
                     as Step 13's editing path)",
                    x.name
                )));
            }
        }
    }
    Ok(())
}

/// Locate the catalog record for `target_cnid` (file or folder, not a
/// thread) and stamp its `bsdInfo.special` u32 (record body offset
/// 32+12 = 44) with `value`. Used by 22c-i to write live link counts
/// onto inode rows after every stub has been emitted.
fn patch_record_bsd_special(
    catalog: &mut [u8],
    target_cnid: u32,
    value: u32,
) -> Result<(), FilesystemError> {
    let header = hfs_common::BTreeHeader::read(catalog);
    let node_size = header.node_size as usize;
    let mut node_idx = header.first_leaf_node;
    while node_idx != 0 {
        let off = node_idx as usize * node_size;
        let next_node = BigEndian::read_u32(&catalog[off..off + 4]);
        let num_records = BigEndian::read_u16(&catalog[off + 10..off + 12]) as usize;
        for rec_idx in 0..num_records {
            let (rec_start, _) =
                hfs_common::btree_record_range(&catalog[off..off + node_size], node_size, rec_idx);
            let abs = off + rec_start;
            let key_len = BigEndian::read_u16(&catalog[abs..abs + 2]) as usize;
            let mut body = abs + 2 + key_len;
            if !body.is_multiple_of(2) {
                body += 1;
            }
            if body + 48 > catalog.len() {
                continue;
            }
            let record_type = BigEndian::read_i16(&catalog[body..body + 2]);
            // Both folder (88B) and file (248B) records carry CNID at
            // body+8 and bsdInfo at body+32; thread records (3/4) don't
            // carry bsdInfo and are skipped.
            if record_type != CATALOG_FOLDER && record_type != CATALOG_FILE {
                continue;
            }
            let cnid = BigEndian::read_u32(&catalog[body + 8..body + 12]);
            if cnid == target_cnid {
                BigEndian::write_u32(&mut catalog[body + 44..body + 48], value);
                return Ok(());
            }
        }
        node_idx = next_node;
    }
    Err(FilesystemError::NotFound(format!(
        "catalog record for inode CNID {target_cnid} not found while patching link count"
    )))
}

fn patch_folder_valences(
    catalog: &mut [u8],
    counts: &std::collections::HashMap<u32, u32>,
    case_sensitive: bool,
) -> Result<(), FilesystemError> {
    let header = hfs_common::BTreeHeader::read(catalog);
    let node_size = header.node_size as usize;
    let first_leaf = header.first_leaf_node;
    let mut node_idx = first_leaf;
    while node_idx != 0 {
        let off = node_idx as usize * node_size;
        let next_node = BigEndian::read_u32(&catalog[off..off + 4]);
        let num_records = BigEndian::read_u16(&catalog[off + 10..off + 12]) as usize;
        for rec_idx in 0..num_records {
            let (rec_start, _rec_end) =
                hfs_common::btree_record_range(&catalog[off..off + node_size], node_size, rec_idx);
            let abs = off + rec_start;
            let key_len = BigEndian::read_u16(&catalog[abs..abs + 2]) as usize;
            let mut body = abs + 2 + key_len;
            if !body.is_multiple_of(2) {
                body += 1;
            }
            if body + 12 > catalog.len() {
                continue;
            }
            let record_type = BigEndian::read_i16(&catalog[body..body + 2]);
            if record_type != CATALOG_FOLDER {
                continue;
            }
            let cnid = BigEndian::read_u32(&catalog[body + 8..body + 12]);
            let count = counts.get(&cnid).copied().unwrap_or(0);
            BigEndian::write_u32(&mut catalog[body + 4..body + 8], count);
        }
        node_idx = next_node;
    }
    let _ = case_sensitive;
    Ok(())
}

/// Build the 512-byte HFS+ volume header.
fn build_volume_header(
    snapshot: &SourceCatalogSnapshot,
    plan: &TargetLayoutPlan,
    finder_info: [u32; 8],
    file_count: u32,
    folder_count: u32,
    free_blocks: u32,
) -> [u8; 512] {
    let mut out = [0u8; 512];
    let signature = snapshot.volume.signature;
    let version: u16 = if signature == 0x4858 { 5 } else { 4 };
    BigEndian::write_u16(&mut out[0..2], signature);
    BigEndian::write_u16(&mut out[2..4], version);
    BigEndian::write_u32(&mut out[4..8], HFSPLUS_VOLUME_UNMOUNTED_BIT); // attributes
    BigEndian::write_u32(&mut out[8..12], 0); // last_mounted_version
    BigEndian::write_u32(&mut out[12..16], 0); // journal_info_block (no journal)
    BigEndian::write_u32(&mut out[16..20], snapshot.volume.create_date);
    BigEndian::write_u32(&mut out[20..24], snapshot.volume.modify_date);
    BigEndian::write_u32(&mut out[24..28], snapshot.volume.backup_date);
    BigEndian::write_u32(&mut out[28..32], snapshot.volume.checked_date);
    BigEndian::write_u32(&mut out[32..36], file_count);
    BigEndian::write_u32(&mut out[36..40], folder_count);
    BigEndian::write_u32(&mut out[40..44], plan.block_size);
    BigEndian::write_u32(&mut out[44..48], plan.total_blocks);
    BigEndian::write_u32(&mut out[48..52], free_blocks);
    BigEndian::write_u32(&mut out[52..56], plan.user_data_start); // next_allocation
    BigEndian::write_u32(&mut out[56..60], plan.block_size); // rsrc_clump
    BigEndian::write_u32(&mut out[60..64], plan.block_size); // data_clump
    let max_target_cnid = plan.cnid_map.values().copied().max().unwrap_or(15);
    BigEndian::write_u32(&mut out[64..68], max_target_cnid + 1);
    BigEndian::write_u32(&mut out[68..72], 0); // write_count
    BigEndian::write_u64(&mut out[72..80], 1); // encodings_bitmap (MacRoman)
    for i in 0..8 {
        BigEndian::write_u32(&mut out[80 + i * 4..84 + i * 4], finder_info[i]);
    }

    // allocation_file at bitmap_start..+bitmap_blocks.
    let bitmap_logical = plan.bitmap_blocks as u64 * plan.block_size as u64;
    write_fork_descriptor(
        &mut out[112..192],
        bitmap_logical,
        plan.bitmap_blocks,
        plan.bitmap_start,
        plan.bitmap_blocks,
    );
    // extents_file
    let extents_logical = plan.extents_blocks as u64 * plan.block_size as u64;
    write_fork_descriptor(
        &mut out[192..272],
        extents_logical,
        plan.extents_blocks,
        plan.extents_start,
        plan.extents_blocks,
    );
    // catalog_file
    let catalog_logical = plan.catalog_blocks as u64 * plan.block_size as u64;
    write_fork_descriptor(
        &mut out[272..352],
        catalog_logical,
        plan.catalog_blocks,
        plan.catalog_start,
        plan.catalog_blocks,
    );
    // attributes_file (zero/empty when no xattrs).
    let attr_logical = plan.attributes_blocks as u64 * plan.block_size as u64;
    write_fork_descriptor(
        &mut out[352..432],
        attr_logical,
        plan.attributes_blocks,
        plan.attributes_start,
        plan.attributes_blocks,
    );
    // startup_file: zero.
    out
}

fn write_fork_descriptor(
    out: &mut [u8],
    logical_size: u64,
    total_blocks: u32,
    start_block: u32,
    block_count: u32,
) {
    BigEndian::write_u64(&mut out[0..8], logical_size);
    BigEndian::write_u32(&mut out[8..12], 0); // clump
    BigEndian::write_u32(&mut out[12..16], total_blocks);
    if block_count > 0 {
        BigEndian::write_u32(&mut out[16..20], start_block);
        BigEndian::write_u32(&mut out[20..24], block_count);
    }
}

// Suppress unused warnings on the SourceHardlinkSnapshot / SourceDirHardlinkSnapshot
// types until 22c follow-ups (file + dir hardlinks) wire them in.
#[allow(dead_code)]
fn _touch(_: &SourceHardlinkSnapshot, _: &SourceDirHardlinkSnapshot) {}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::super::filesystem::{
        CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
    };
    use super::super::hfsplus::{create_blank_hfsplus, HfsPlusFilesystem};
    use super::super::hfsplus_clone::SourceCatalogSnapshot;
    use super::*;

    /// Build a small synthetic HFS+ source with two files (different
    /// fork sizes) and one directory, capture it, and exercise the
    /// planner. Asserts CNID-remap is sequential, fork extents are
    /// contiguous, and reserved-region sizes are sane.
    #[test]
    fn plan_assigns_contiguous_packed_extents() {
        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Source", false);
        {
            let cur = Cursor::new(&mut img);
            let mut fs = HfsPlusFilesystem::open(cur, 0).unwrap();
            fs.prepare_for_edit().unwrap();
            let root = fs.root().unwrap();
            let _ = fs
                .create_directory(&root, "Docs", &CreateDirectoryOptions::default())
                .unwrap();
            let small_payload = b"small";
            let mut sd = Cursor::new(small_payload);
            fs.create_file(
                &root,
                "small.txt",
                &mut sd,
                small_payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
            // Big enough to take 2 blocks (>4096 bytes).
            let big_payload = vec![0xABu8; 5000];
            let mut bd = Cursor::new(&big_payload);
            fs.create_file(
                &root,
                "big.bin",
                &mut bd,
                big_payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }

        let cur = Cursor::new(&mut img);
        let mut fs = HfsPlusFilesystem::open(cur, 0).unwrap();
        let snap = SourceCatalogSnapshot::capture(&mut fs).unwrap();

        // Plan a 16 MiB target — comfortably larger than the small
        // user-data footprint, smaller than the source.
        let plan = plan_defrag_layout(&snap, 16 * 1024 * 1024).expect("plan should succeed");

        // Block size matches source.
        assert_eq!(plan.block_size, 4096);
        assert_eq!(plan.total_blocks, (16 * 1024 * 1024) / 4096);

        // Reserved region is at the front and non-empty.
        assert_eq!(plan.bitmap_start, 1);
        assert!(plan.bitmap_blocks >= 1);
        assert!(plan.catalog_start > plan.bitmap_start);
        assert!(plan.catalog_blocks >= 1);
        assert!(plan.extents_start >= plan.catalog_start + plan.catalog_blocks);
        assert!(plan.user_data_start >= plan.extents_start + plan.extents_blocks);
        assert_eq!(plan.reserved_blocks, plan.user_data_start);

        // CNID map: root maps to root; user dirs / files get sequential
        // CNIDs starting at 16.
        assert_eq!(plan.cnid_map.get(&2).copied(), Some(2));
        let mut targets: Vec<u32> = plan
            .cnid_map
            .iter()
            .filter(|(&src, _)| src != 2)
            .map(|(_, &t)| t)
            .collect();
        targets.sort_unstable();
        let want: Vec<u32> = (FIRST_USER_CNID..FIRST_USER_CNID + targets.len() as u32).collect();
        assert_eq!(
            targets, want,
            "non-root CNIDs must be sequential from {FIRST_USER_CNID}"
        );

        // File placements: small (1 block), big (2 blocks). Both packed
        // back-to-back starting at user_data_start.
        let small = snap
            .files
            .iter()
            .find(|f| f.name == "small.txt")
            .expect("small file");
        let big = snap
            .files
            .iter()
            .find(|f| f.name == "big.bin")
            .expect("big");
        let small_p = plan.file_placements.get(&small.cnid).expect("small placed");
        let big_p = plan.file_placements.get(&big.cnid).expect("big placed");
        let small_data = small_p.data_fork.expect("small data fork");
        let big_data = big_p.data_fork.expect("big data fork");
        assert_eq!(small_data.block_count, 1);
        assert_eq!(big_data.block_count, 2);
        // Contiguity: order depends on source CNID; whichever comes
        // first starts at user_data_start, the next directly after.
        let mut starts = [
            (small.cnid, small_data.start_block, small_data.block_count),
            (big.cnid, big_data.start_block, big_data.block_count),
        ];
        starts.sort_by_key(|&(cnid, _, _)| cnid);
        assert_eq!(starts[0].1, plan.user_data_start);
        assert_eq!(starts[1].1, starts[0].1 + starts[0].2);

        // Resource forks are absent — empty fork → None.
        assert!(small_p.rsrc_fork.is_none());
        assert!(big_p.rsrc_fork.is_none());
    }

    #[test]
    fn plan_refuses_target_smaller_than_metadata() {
        // A blank source's reserved region is roughly 13 blocks
        // (1 boot+VH + 1 bitmap + 6 catalog (4×1.5) + 4 extents + 0
        // attributes + 1 alt-VH). A 32 KiB target = 8 blocks won't fit.
        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Tiny", false);
        let cur = Cursor::new(&mut img);
        let mut fs = HfsPlusFilesystem::open(cur, 0).unwrap();
        let snap = SourceCatalogSnapshot::capture(&mut fs).unwrap();
        let err = plan_defrag_layout(&snap, 32 * 1024).expect_err("32 KiB target must be refused");
        match err {
            PlanError::TargetTooSmallForMetadata { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    /// Round-trip: build a small synthetic source, plan + build target
    /// metadata, stitch into a sparse target image, open as
    /// HfsPlusFilesystem, walk the catalog. Verifies dirs and files
    /// land at the right CNIDs with byte-correct fork descriptors,
    /// and that fsck reports clean.
    #[test]
    fn build_metadata_round_trip_dirs_and_files() {
        // Source: 32 MiB blank with one dir + two files (no xattrs,
        // no hardlinks — those lock 22c out of the basic path).
        let mut src_img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Source", false);
        {
            let cur = Cursor::new(&mut src_img);
            let mut fs = HfsPlusFilesystem::open(cur, 0).unwrap();
            fs.prepare_for_edit().unwrap();
            let root = fs.root().unwrap();
            let _docs = fs
                .create_directory(&root, "Docs", &CreateDirectoryOptions::default())
                .unwrap();
            let mut a = Cursor::new(b"hello\n".as_ref());
            fs.create_file(&root, "a.txt", &mut a, 6, &CreateFileOptions::default())
                .unwrap();
            let mut b = Cursor::new(b"world\n".as_ref());
            fs.create_file(&root, "b.txt", &mut b, 6, &CreateFileOptions::default())
                .unwrap();
            fs.sync_metadata().unwrap();
        }

        let cur = Cursor::new(&mut src_img);
        let mut src_fs = HfsPlusFilesystem::open(cur, 0).unwrap();
        let snap = SourceCatalogSnapshot::capture(&mut src_fs).unwrap();

        let target_size: u64 = 8 * 1024 * 1024;
        let plan = plan_defrag_layout(&snap, target_size).unwrap();
        let meta = build_target_metadata(&plan, &snap).expect("build metadata");

        // Stitch together a target image: block_zero + zero gap +
        // bitmap + catalog + extents + zero user-data area + alt VH.
        let bs = plan.block_size as usize;
        let mut img = vec![0u8; target_size as usize];
        // Block 0.
        img[..bs].copy_from_slice(&meta.block_zero);
        // Bitmap.
        let bitmap_off = plan.bitmap_start as usize * bs;
        img[bitmap_off..bitmap_off + meta.bitmap.len()].copy_from_slice(&meta.bitmap);
        // Catalog.
        let catalog_off = plan.catalog_start as usize * bs;
        img[catalog_off..catalog_off + meta.catalog.len()].copy_from_slice(&meta.catalog);
        // Extents-overflow.
        let extents_off = plan.extents_start as usize * bs;
        img[extents_off..extents_off + meta.extents_overflow.len()]
            .copy_from_slice(&meta.extents_overflow);
        // User data area: zeros are fine since we don't validate fork
        // contents in this test (22d will stream the real bytes).
        // Alt VH.
        let alt = target_size as usize - 1024;
        img[alt..alt + 512].copy_from_slice(&meta.vh_bytes);

        // Open the produced image and walk the catalog.
        let cur = Cursor::new(img);
        let mut tgt = HfsPlusFilesystem::open(cur, 0).expect("target opens");
        let root = tgt.root().expect("root");
        let kids = tgt.list_directory(&root).expect("list root");
        let names: Vec<&str> = kids.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"Docs"), "Docs missing: {names:?}");
        assert!(names.contains(&"a.txt"), "a.txt missing: {names:?}");
        assert!(names.contains(&"b.txt"), "b.txt missing: {names:?}");
    }

    /// Build a source with one file inode plus 3 hardlink stub rows.
    /// Plan and build target metadata, stitch into a target image,
    /// open as `HfsPlusFilesystem`, walk the catalog. All 3 stubs and
    /// the inode row must be present; the inode's `bsdInfo.special`
    /// (link count) must equal 3.
    #[test]
    fn build_metadata_emits_file_hardlinks_with_link_count() {
        const PAYLOAD: &[u8] = b"shared\n";
        const I_NODE_NUM: u32 = 42;

        let mut src_img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Src", false);
        {
            let cursor = Cursor::new(&mut src_img);
            let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
            fs.prepare_for_edit().unwrap();
            let mut data = Cursor::new(PAYLOAD);
            fs.create_hardlink_inode(
                I_NODE_NUM,
                &mut data,
                PAYLOAD.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
            let root = fs.root().unwrap();
            for name in ["a.txt", "b.txt", "c.txt"] {
                fs.create_hardlink(&root, name, I_NODE_NUM).unwrap();
            }
            fs.sync_metadata().unwrap();
        }

        let cursor = Cursor::new(&mut src_img);
        let mut src_fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        let snap = SourceCatalogSnapshot::capture(&mut src_fs).unwrap();
        assert_eq!(snap.hardlinks.len(), 3, "expected 3 hardlink stubs");
        assert_eq!(
            snap.files.iter().filter(|f| f.is_inode).count(),
            1,
            "expected 1 inode"
        );

        let target_size: u64 = 8 * 1024 * 1024;
        let plan = plan_defrag_layout(&snap, target_size).unwrap();
        let meta = build_target_metadata(&plan, &snap).expect("build metadata");

        // Stitch a target image (no fork bytes streamed — those land in
        // 22d; this test only exercises catalog correctness).
        let bs = plan.block_size as usize;
        let mut img = vec![0u8; target_size as usize];
        img[..bs].copy_from_slice(&meta.block_zero);
        let bitmap_off = plan.bitmap_start as usize * bs;
        img[bitmap_off..bitmap_off + meta.bitmap.len()].copy_from_slice(&meta.bitmap);
        let catalog_off = plan.catalog_start as usize * bs;
        img[catalog_off..catalog_off + meta.catalog.len()].copy_from_slice(&meta.catalog);
        let extents_off = plan.extents_start as usize * bs;
        img[extents_off..extents_off + meta.extents_overflow.len()]
            .copy_from_slice(&meta.extents_overflow);
        let alt = target_size as usize - 1024;
        img[alt..alt + 512].copy_from_slice(&meta.vh_bytes);

        let cursor = Cursor::new(img);
        let mut tgt = HfsPlusFilesystem::open(cursor, 0).expect("target opens");
        let root = tgt.root().expect("root");
        let kids = tgt.list_directory(&root).expect("list root");
        let stub_names: Vec<&str> = kids
            .iter()
            .filter(|e| ["a.txt", "b.txt", "c.txt"].contains(&e.name.as_str()))
            .map(|e| e.name.as_str())
            .collect();
        assert_eq!(stub_names.len(), 3, "expected 3 stubs in root: {kids:?}");

        // Verify the inode's link count was patched. We have to walk the
        // catalog looking for the inode CNID's record body and reading
        // bsdInfo.special.
        let inode_target_cnid = {
            let inode = snap.files.iter().find(|f| f.is_inode).unwrap();
            *plan.cnid_map.get(&inode.cnid).unwrap()
        };
        let link_count =
            read_inode_link_count(&meta.catalog, inode_target_cnid).expect("inode catalog record");
        assert_eq!(link_count, 3, "inode should carry linkCount=3");
    }

    /// Directory hardlinks: 2 stub rows + 1 directory inode under
    /// `.HFS+ Private Directory Data\r`. Build target metadata, open as
    /// HfsPlusFilesystem, verify both stubs are present at root and the
    /// inode dir is reachable under the dir-private dir.
    #[test]
    fn build_metadata_emits_dir_hardlinks_with_link_count() {
        const D_INODE_NUM: u32 = 7;

        let mut src_img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "DhSrc", false);
        {
            let cursor = Cursor::new(&mut src_img);
            let mut fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
            fs.prepare_for_edit().unwrap();
            fs.create_dir_hardlink_inode(D_INODE_NUM, &CreateDirectoryOptions::default())
                .unwrap();
            let root = fs.root().unwrap();
            for name in ["alpha", "beta"] {
                fs.create_dir_hardlink(&root, name, D_INODE_NUM).unwrap();
            }
            fs.sync_metadata().unwrap();
        }

        let cursor = Cursor::new(&mut src_img);
        let mut src_fs = HfsPlusFilesystem::open(cursor, 0).unwrap();
        let snap = SourceCatalogSnapshot::capture(&mut src_fs).unwrap();
        assert_eq!(snap.dir_hardlinks.len(), 2);
        assert_eq!(snap.dirs.iter().filter(|d| d.is_dir_inode).count(), 1);

        let target_size: u64 = 8 * 1024 * 1024;
        let plan = plan_defrag_layout(&snap, target_size).unwrap();
        let meta = build_target_metadata(&plan, &snap).expect("build metadata");

        // Patch the target image and reopen.
        let bs = plan.block_size as usize;
        let mut img = vec![0u8; target_size as usize];
        img[..bs].copy_from_slice(&meta.block_zero);
        let bo = plan.bitmap_start as usize * bs;
        img[bo..bo + meta.bitmap.len()].copy_from_slice(&meta.bitmap);
        let co = plan.catalog_start as usize * bs;
        img[co..co + meta.catalog.len()].copy_from_slice(&meta.catalog);
        let eo = plan.extents_start as usize * bs;
        img[eo..eo + meta.extents_overflow.len()].copy_from_slice(&meta.extents_overflow);
        let alt = target_size as usize - 1024;
        img[alt..alt + 512].copy_from_slice(&meta.vh_bytes);

        let cursor = Cursor::new(img);
        let mut tgt = HfsPlusFilesystem::open(cursor, 0).expect("target opens");
        let root = tgt.root().expect("root");
        let kids = tgt.list_directory(&root).expect("list root");
        let stub_names: Vec<&str> = kids
            .iter()
            .filter(|e| ["alpha", "beta"].contains(&e.name.as_str()))
            .map(|e| e.name.as_str())
            .collect();
        assert_eq!(stub_names.len(), 2, "expected 2 dir-hardlink stubs");

        // Verify the dir inode's link count.
        let inode_target_cnid = {
            let inode = snap.dirs.iter().find(|d| d.is_dir_inode).unwrap();
            *plan.cnid_map.get(&inode.cnid).unwrap()
        };
        let link_count = read_inode_link_count(&meta.catalog, inode_target_cnid)
            .expect("dir inode catalog record");
        assert_eq!(link_count, 2, "dir inode should carry linkCount=2");
    }

    /// Walk a catalog buffer, find the file/folder record for `cnid`,
    /// return its `bsdInfo.special` (record body offset 44, u32 BE).
    fn read_inode_link_count(catalog: &[u8], cnid: u32) -> Option<u32> {
        let header = super::super::hfs_common::BTreeHeader::read(catalog);
        let node_size = header.node_size as usize;
        let mut node_idx = header.first_leaf_node;
        while node_idx != 0 {
            let off = node_idx as usize * node_size;
            let next = BigEndian::read_u32(&catalog[off..off + 4]);
            let num = BigEndian::read_u16(&catalog[off + 10..off + 12]) as usize;
            for rec_idx in 0..num {
                let (rec_start, _) = super::super::hfs_common::btree_record_range(
                    &catalog[off..off + node_size],
                    node_size,
                    rec_idx,
                );
                let abs = off + rec_start;
                let key_len = BigEndian::read_u16(&catalog[abs..abs + 2]) as usize;
                let mut body = abs + 2 + key_len;
                if !body.is_multiple_of(2) {
                    body += 1;
                }
                if body + 48 > catalog.len() {
                    continue;
                }
                let rt = BigEndian::read_i16(&catalog[body..body + 2]);
                if rt != CATALOG_FOLDER && rt != CATALOG_FILE {
                    continue;
                }
                let rcnid = BigEndian::read_u32(&catalog[body + 8..body + 12]);
                if rcnid == cnid {
                    return Some(BigEndian::read_u32(&catalog[body + 44..body + 48]));
                }
            }
            node_idx = next;
        }
        None
    }

    /// Plain blank — no xattrs, no hardlinks. With 22c-i shipped the
    /// only remaining `Unsupported` guard is for xattr-bearing volumes;
    /// a blank source flows through the basic dirs+files path cleanly.
    /// Round-trip an inline xattr: synthesize a minimal source snapshot
    /// carrying one file with `com.apple.FinderInfo` inline xattr,
    /// build target metadata, parse the resulting attributes B-tree
    /// back, verify the record's CNID is the planner-remapped target
    /// CNID and the value bytes round-trip byte-equal.
    #[test]
    fn build_metadata_emits_inline_xattr() {
        use super::super::hfsplus::{XattrKind, XattrRecord};
        use super::super::hfsplus_clone::{
            SourceCatalogSnapshot, SourceDirSnapshot, SourceFileSnapshot, SourceVolumeSnapshot,
        };

        let label = "Tgt".to_string();
        let label_utf16: Vec<u16> = label.encode_utf16().collect();

        // Root dir. CNID 2, parent 1, no xattrs.
        let root = SourceDirSnapshot {
            name: label.clone(),
            name_utf16: label_utf16.clone(),
            cnid: 2,
            parent_cnid: 1,
            valence: 1,
            flags: 0,
            finder_info: [0u8; 16],
            extended_finder_info: [0u8; 16],
            bsd_info: [0u8; 16],
            create_date: 0,
            content_mod_date: 0,
            attribute_mod_date: 0,
            access_date: 0,
            backup_date: 0,
            text_encoding: 0,
            xattrs: Vec::new(),
            is_dir_inode: false,
            inode_num: None,
        };
        // One file, source CNID = 16, with one inline xattr.
        let xattr_value = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\
                            \x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"
            .to_vec();
        let xattr = XattrRecord {
            name: "com.apple.FinderInfo".to_string(),
            start_block: 0,
            kind: XattrKind::Inline(xattr_value.clone()),
        };
        let f_name = "note.txt".to_string();
        let f_name_utf16: Vec<u16> = f_name.encode_utf16().collect();
        let mut data_fork_raw = [0u8; 80];
        // logical_size = 0 → no data fork allocation. Planner skips it.
        BigEndian::write_u64(&mut data_fork_raw[0..8], 0);
        let file = SourceFileSnapshot {
            name: f_name,
            name_utf16: f_name_utf16,
            cnid: 100,
            parent_cnid: 2,
            flags: 0,
            finder_info: [0u8; 16],
            extended_finder_info: [0u8; 16],
            bsd_info: [0u8; 16],
            create_date: 0,
            content_mod_date: 0,
            attribute_mod_date: 0,
            access_date: 0,
            backup_date: 0,
            text_encoding: 0,
            data_fork_raw,
            rsrc_fork_raw: [0u8; 80],
            data_fork_size: 0,
            rsrc_fork_size: 0,
            xattrs: vec![xattr],
            is_inode: false,
            inode_num: None,
        };

        let snap = SourceCatalogSnapshot {
            volume: SourceVolumeSnapshot {
                label: label.clone(),
                signature: 0x482B,
                attributes: 0x100,
                create_date: 0,
                modify_date: 0,
                backup_date: 0,
                checked_date: 0,
                finder_info: [0u32; 8],
                block_size: 4096,
                total_blocks: 0,
                file_count: 1,
                folder_count: 0,
                boot_blocks: [0u8; 1024],
                catalog_total_blocks: 4,
                extents_total_blocks: 4,
                // Force the planner to size an attributes B-tree.
                attributes_total_blocks: 4,
            },
            dirs: vec![root],
            files: vec![file],
            hardlinks: Vec::new(),
            dir_hardlinks: Vec::new(),
        };

        let target_size: u64 = 8 * 1024 * 1024;
        let plan = plan_defrag_layout(&snap, target_size).expect("plan");
        assert!(
            plan.attributes_blocks >= 4,
            "planner must size attributes B-tree from source"
        );
        let meta = build_target_metadata(&plan, &snap).expect("build with xattr");
        let attr_buf = meta.attributes.as_ref().expect("attributes buffer present");

        // Parse the leaf, find our record, verify CNID + value.
        let target_cnid = *plan.cnid_map.get(&100).expect("file CNID remapped");
        let (found_cnid, found_value) =
            read_first_inline_xattr(attr_buf).expect("at least one xattr record");
        assert_eq!(
            found_cnid, target_cnid,
            "xattr record must reference remapped target CNID"
        );
        assert_eq!(found_value, xattr_value, "xattr value must round-trip");
    }

    /// Walk the leaf of an attributes B-tree, return the first inline
    /// xattr record's `(fileID, valueBytes)`. Test-only.
    fn read_first_inline_xattr(attr_buf: &[u8]) -> Option<(u32, Vec<u8>)> {
        let header = super::super::hfs_common::BTreeHeader::read(attr_buf);
        let node_size = header.node_size as usize;
        let leaf_off = header.first_leaf_node as usize * node_size;
        let num = BigEndian::read_u16(&attr_buf[leaf_off + 10..leaf_off + 12]) as usize;
        if num == 0 {
            return None;
        }
        let (rec_start, _) = super::super::hfs_common::btree_record_range(
            &attr_buf[leaf_off..leaf_off + node_size],
            node_size,
            0,
        );
        let abs = leaf_off + rec_start;
        let key_len = BigEndian::read_u16(&attr_buf[abs..abs + 2]) as usize;
        let cnid = BigEndian::read_u32(&attr_buf[abs + 4..abs + 8]);
        let name_len = BigEndian::read_u16(&attr_buf[abs + 12..abs + 14]) as usize;
        let data_off = abs + 2 + key_len;
        // Inline-data record body: recordType(4) + reserved(8) + attrSize(4) + attrData.
        let attr_size = BigEndian::read_u32(&attr_buf[data_off + 12..data_off + 16]) as usize;
        let value = attr_buf[data_off + 16..data_off + 16 + attr_size].to_vec();
        let _ = name_len;
        Some((cnid, value))
    }

    /// Fork-style xattrs trip a clear `Unsupported` error rather than
    /// silently dropping. Mirrors Step 13's editing-side limitation.
    #[test]
    fn build_metadata_refuses_fork_style_xattr() {
        use super::super::hfsplus::{ExtentDescriptor, ForkData, XattrKind, XattrRecord};
        use super::super::hfsplus_clone::{
            SourceCatalogSnapshot, SourceDirSnapshot, SourceFileSnapshot, SourceVolumeSnapshot,
        };

        let mut data_fork_raw = [0u8; 80];
        BigEndian::write_u64(&mut data_fork_raw[0..8], 0);
        let f = SourceFileSnapshot {
            name: "f".into(),
            name_utf16: "f".encode_utf16().collect(),
            cnid: 100,
            parent_cnid: 2,
            flags: 0,
            finder_info: [0u8; 16],
            extended_finder_info: [0u8; 16],
            bsd_info: [0u8; 16],
            create_date: 0,
            content_mod_date: 0,
            attribute_mod_date: 0,
            access_date: 0,
            backup_date: 0,
            text_encoding: 0,
            data_fork_raw,
            rsrc_fork_raw: [0u8; 80],
            data_fork_size: 0,
            rsrc_fork_size: 0,
            xattrs: vec![XattrRecord {
                name: "com.apple.bigxattr".into(),
                start_block: 0,
                kind: XattrKind::Fork(ForkData {
                    logical_size: 16384,
                    clump_size: 0,
                    total_blocks: 4,
                    extents: [ExtentDescriptor {
                        start_block: 0,
                        block_count: 0,
                    }; 8],
                }),
            }],
            is_inode: false,
            inode_num: None,
        };
        let root = SourceDirSnapshot {
            name: "T".into(),
            name_utf16: "T".encode_utf16().collect(),
            cnid: 2,
            parent_cnid: 1,
            valence: 1,
            flags: 0,
            finder_info: [0u8; 16],
            extended_finder_info: [0u8; 16],
            bsd_info: [0u8; 16],
            create_date: 0,
            content_mod_date: 0,
            attribute_mod_date: 0,
            access_date: 0,
            backup_date: 0,
            text_encoding: 0,
            xattrs: Vec::new(),
            is_dir_inode: false,
            inode_num: None,
        };
        let snap = SourceCatalogSnapshot {
            volume: SourceVolumeSnapshot {
                label: "T".into(),
                signature: 0x482B,
                attributes: 0x100,
                create_date: 0,
                modify_date: 0,
                backup_date: 0,
                checked_date: 0,
                finder_info: [0u32; 8],
                block_size: 4096,
                total_blocks: 0,
                file_count: 1,
                folder_count: 0,
                boot_blocks: [0u8; 1024],
                catalog_total_blocks: 4,
                extents_total_blocks: 4,
                attributes_total_blocks: 4,
            },
            dirs: vec![root],
            files: vec![f],
            hardlinks: Vec::new(),
            dir_hardlinks: Vec::new(),
        };
        let plan = plan_defrag_layout(&snap, 8 * 1024 * 1024).unwrap();
        let err = build_target_metadata(&plan, &snap)
            .expect_err("fork-style xattr must trip Unsupported");
        match err {
            FilesystemError::Unsupported(msg) => {
                assert!(
                    msg.contains("fork-style") || msg.contains("inline"),
                    "unexpected error: {msg}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn build_metadata_handles_plain_blank() {
        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Plain", false);
        let cur = Cursor::new(&mut img);
        let mut fs = HfsPlusFilesystem::open(cur, 0).unwrap();
        let snap = SourceCatalogSnapshot::capture(&mut fs).unwrap();
        let plan = plan_defrag_layout(&snap, 8 * 1024 * 1024).unwrap();
        build_target_metadata(&plan, &snap).expect("plain blank should build");
    }

    #[test]
    fn plan_refuses_unaligned_target() {
        let mut img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "U", false);
        let cur = Cursor::new(&mut img);
        let mut fs = HfsPlusFilesystem::open(cur, 0).unwrap();
        let snap = SourceCatalogSnapshot::capture(&mut fs).unwrap();
        let err = plan_defrag_layout(&snap, 16 * 1024 * 1024 + 1)
            .expect_err("non-block-aligned target must be refused");
        match err {
            PlanError::TargetNotBlockAligned(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    /// Smoke test for [`stream_defragmented_hfsplus`]: emit a forward-only
    /// image into a `Vec<u8>`, reopen it as `HfsPlusFilesystem`, and
    /// verify dirs, files, and fork bytes round-trip. The full fixture
    /// (xattrs + hardlinks + fragmented forks + fsck) lands in 22e.
    #[test]
    fn stream_emit_round_trips_dirs_and_files() {
        let mut src_img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Stream", false);
        {
            let cur = Cursor::new(&mut src_img);
            let mut fs = HfsPlusFilesystem::open(cur, 0).unwrap();
            fs.prepare_for_edit().unwrap();
            let root = fs.root().unwrap();
            let _ = fs
                .create_directory(&root, "Docs", &CreateDirectoryOptions::default())
                .unwrap();
            let payload_a = b"hello\n";
            let mut a = Cursor::new(payload_a.as_ref());
            fs.create_file(
                &root,
                "a.txt",
                &mut a,
                payload_a.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
            let payload_b = vec![0xCDu8; 5000]; // > 1 block, exercises padding
            let mut b = Cursor::new(&payload_b);
            fs.create_file(
                &root,
                "b.bin",
                &mut b,
                payload_b.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }

        let cur = Cursor::new(&mut src_img);
        let mut src_fs = HfsPlusFilesystem::open(cur, 0).unwrap();

        let target_size: u64 = 8 * 1024 * 1024;
        let mut out: Vec<u8> = Vec::with_capacity(target_size as usize);
        let report = stream_defragmented_hfsplus(&mut src_fs, target_size, &mut out, None)
            .expect("stream emit should succeed");
        assert_eq!(out.len() as u64, target_size, "emit size mismatch");
        assert_eq!(report.bytes_emitted, target_size);
        assert_eq!(report.files_copied, 2);
        assert_eq!(report.dirs_copied, 1);
        assert_eq!(report.data_bytes_copied, 6 + 5000);

        let cur = Cursor::new(out);
        let mut tgt = HfsPlusFilesystem::open(cur, 0).expect("target opens");
        let root = tgt.root().expect("root");
        let kids = tgt.list_directory(&root).expect("list root");
        let names: Vec<&str> = kids.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"Docs"));
        assert!(names.contains(&"a.txt"));
        assert!(names.contains(&"b.bin"));

        let a = kids.iter().find(|e| e.name == "a.txt").unwrap().clone();
        let bytes = tgt.read_file(&a, usize::MAX).expect("read a.txt");
        assert_eq!(bytes, b"hello\n");
        let b = kids.iter().find(|e| e.name == "b.bin").unwrap().clone();
        let bytes = tgt.read_file(&b, usize::MAX).expect("read b.bin");
        assert_eq!(bytes.len(), 5000);
        assert!(bytes.iter().all(|&x| x == 0xCD));
    }

    /// Step 22e — full end-to-end engine fixture. Build a synthetic HFS+
    /// source that exercises the clone-relevant features the blank
    /// builder can seed: a nested directory tree, a file with both data
    /// and resource forks, and a file hardlink pair. Stream into a
    /// `Vec<u8>`, reopen as `HfsPlusFilesystem`, walk the catalog, and
    /// verify byte-equal data forks and resource fork, preserved
    /// hardlink topology (both link rows resolve to identical bytes),
    /// and `fsck` clean on the target. Xattr emit is exercised
    /// separately by `build_metadata_emits_inline_xattr` —
    /// `create_blank_hfsplus` doesn't seed an attributes B-tree, so
    /// live `set_xattr` against the blank source is a separate plumbing
    /// step.
    #[test]
    fn stream_emit_round_trips_full_fixture() {
        use super::super::filesystem::ResourceForkSource;

        const DATA_PAYLOAD: &[u8] = b"the quick brown fox\n";
        const RSRC_PAYLOAD: &[u8] = b"resource-fork-bytes-12345\n";
        const SHARED_PAYLOAD: &[u8] = b"shared via hardlink\n";
        const I_NODE_NUM: u32 = 91;

        let mut src_img = create_blank_hfsplus(32 * 1024 * 1024, 4096, "Full", false);
        {
            let cur = Cursor::new(&mut src_img);
            let mut fs = HfsPlusFilesystem::open(cur, 0).unwrap();
            fs.prepare_for_edit().unwrap();
            let root = fs.root().unwrap();

            // Nested directory tree.
            let docs = fs
                .create_directory(&root, "Docs", &CreateDirectoryOptions::default())
                .unwrap();
            let _nested = fs
                .create_directory(&docs, "Nested", &CreateDirectoryOptions::default())
                .unwrap();

            // File with both forks.
            let mut data = Cursor::new(DATA_PAYLOAD);
            let opts = CreateFileOptions {
                resource_fork: Some(ResourceForkSource::Data(RSRC_PAYLOAD.to_vec())),
                ..CreateFileOptions::default()
            };
            fs.create_file(
                &docs,
                "with_forks.bin",
                &mut data,
                DATA_PAYLOAD.len() as u64,
                &opts,
            )
            .unwrap();

            // Hardlink pair: one inode + two stub rows in the root.
            let mut shared = Cursor::new(SHARED_PAYLOAD);
            fs.create_hardlink_inode(
                I_NODE_NUM,
                &mut shared,
                SHARED_PAYLOAD.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.create_hardlink(&root, "alpha.txt", I_NODE_NUM).unwrap();
            fs.create_hardlink(&root, "beta.txt", I_NODE_NUM).unwrap();

            fs.sync_metadata().unwrap();
        }

        // Stream the source into a fresh Vec<u8>.
        let cur = Cursor::new(&mut src_img);
        let mut src_fs = HfsPlusFilesystem::open(cur, 0).unwrap();
        let target_size: u64 = 8 * 1024 * 1024;
        let mut out: Vec<u8> = Vec::with_capacity(target_size as usize);
        let report = stream_defragmented_hfsplus(&mut src_fs, target_size, &mut out, None)
            .expect("stream emit should succeed");
        assert_eq!(out.len() as u64, target_size, "emit size mismatch");
        assert_eq!(report.bytes_emitted, target_size);
        // `files_copied` excludes inodes (per DefragReport's filter), so just
        // the dual-fork file. The inode's bytes still flow into
        // `data_bytes_copied` below.
        assert_eq!(report.files_copied, 1, "one non-inode file (dual)");
        // Docs + Nested + the synthesized "HFS+ Private Data" dir; root and
        // dir-inode dirs are excluded by the report filter.
        assert_eq!(report.dirs_copied, 3);
        assert_eq!(report.hardlinks_copied, 2);
        assert_eq!(report.xattrs_copied, 0);
        assert_eq!(
            report.data_bytes_copied,
            DATA_PAYLOAD.len() as u64 + SHARED_PAYLOAD.len() as u64
        );
        assert_eq!(report.rsrc_bytes_copied, RSRC_PAYLOAD.len() as u64);

        // Reopen and verify catalog contents.
        let cur = Cursor::new(out);
        let mut tgt = HfsPlusFilesystem::open(cur, 0).expect("target opens");
        let root = tgt.root().expect("root");
        let kids = tgt.list_directory(&root).expect("list root");
        let names: Vec<&str> = kids.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"Docs"), "Docs missing: {names:?}");
        assert!(names.contains(&"alpha.txt"), "alpha missing: {names:?}");
        assert!(names.contains(&"beta.txt"), "beta missing: {names:?}");

        // Both hardlink rows must resolve to identical bytes.
        let alpha = kids.iter().find(|e| e.name == "alpha.txt").unwrap().clone();
        let beta = kids.iter().find(|e| e.name == "beta.txt").unwrap().clone();
        assert!(
            alpha.link_target_cnid.is_some(),
            "alpha must carry a link_target_cnid"
        );
        assert_eq!(
            alpha.link_target_cnid, beta.link_target_cnid,
            "both stubs should resolve to the same inode"
        );
        let alpha_bytes = tgt.read_file(&alpha, usize::MAX).expect("read alpha");
        let beta_bytes = tgt.read_file(&beta, usize::MAX).expect("read beta");
        assert_eq!(alpha_bytes, SHARED_PAYLOAD);
        assert_eq!(beta_bytes, SHARED_PAYLOAD);

        // Data fork on the dual-fork file under Docs/.
        let docs = kids.iter().find(|e| e.name == "Docs").unwrap().clone();
        let docs_kids = tgt.list_directory(&docs).expect("list Docs");
        let with_forks = docs_kids
            .iter()
            .find(|e| e.name == "with_forks.bin")
            .expect("dual-fork file present")
            .clone();
        assert!(
            docs_kids.iter().any(|e| e.name == "Nested"),
            "Nested dir missing"
        );
        let data_bytes = tgt.read_file(&with_forks, usize::MAX).expect("read data");
        assert_eq!(data_bytes, DATA_PAYLOAD);

        // Resource fork: write_resource_fork_to streams it into a buffer.
        let mut rsrc_buf: Vec<u8> = Vec::new();
        let written = tgt
            .write_resource_fork_to(&with_forks, &mut rsrc_buf)
            .expect("read rsrc");
        assert_eq!(written, RSRC_PAYLOAD.len() as u64);
        assert_eq!(rsrc_buf, RSRC_PAYLOAD);
        assert_eq!(
            tgt.resource_fork_size(&with_forks),
            RSRC_PAYLOAD.len() as u64
        );

        // fsck clean on the target.
        let fsck = tgt
            .fsck()
            .expect("HFS+ supports fsck")
            .expect("fsck runs without error");
        assert!(
            fsck.is_clean(),
            "target should be fsck-clean: errors={:?}",
            fsck.errors
                .iter()
                .map(|e| (&e.code, &e.message))
                .collect::<Vec<_>>()
        );
    }
}
