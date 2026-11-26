//! File-aware NTFS images: the run index, the lazy-scan cache and the decompressing reader.

use super::*;

// ---------------------------------------------------------------------------
// NTFS file-aware GHO support
// ---------------------------------------------------------------------------
//
// NTFS file-aware Ghost backups use a completely different internal format
// from FAT file-aware ones. Instead of the 0x012F18D8 record stream, they
// store:
//   1. GHPR metadata blocks containing the NTFS VBR
//   2. Packed cluster runs — each preceded by a 42-byte header containing
//      the cluster count, with MFT FILE records inline between runs
//      providing the LCN mapping
//   3. A single 0x0023 End record at the very end
//
// Detection: if `find_inner_stream_start` fails for a FileAware GHO, we
// scan for the NTFS VBR signature in the header region. If found, we take
// the NTFS path.

/// One compressed zlib block in the NTFS file-aware compressed stream.
#[derive(Debug, Clone)]
pub(super) struct NtfsCompressedBlock {
    pub(super) file_offset: u64,
    pub(super) comp_size: u32,
    pub(super) decomp_offset: u64,
    pub(super) decomp_size: u32,
}

/// State for on-demand decompression of compressed NTFS file-aware GHOs.
/// The decompressed stream is structurally identical to the uncompressed
/// format (same run headers, MFT records, cluster data) — each zlib block
/// decompresses to 10–32768 bytes. Blocks are separated by 2-byte gaps.
pub(super) struct NtfsCompressedState {
    pub(super) blocks: Vec<NtfsCompressedBlock>,
    pub(super) compression: GhoCompression,
    pub(super) total_decompressed: u64,
    pub(super) cache_block: Option<usize>,
    pub(super) cache_buf: Vec<u8>,
    /// Lazy scanning state: where the next forward scan should resume
    /// in the compressed file. `None` = fully scanned.
    pub(super) lazy_scan: Option<NtfsLazyScanState>,
    /// Identity of the source archive for the process-wide scan cache. When
    /// set, the completed run index + block table are stored under this key so
    /// a later re-open of the same archive skips the rescan. `None` disables
    /// caching (e.g. file metadata couldn't be read at open time).
    pub(super) cache_key: Option<NtfsScanKey>,
}

/// State preserved between incremental forward scans of a compressed
/// NTFS file-aware GHO. Allows `read_ntfs_file_aware_into` to extend
/// the run index on demand when an unmapped LCN is accessed.
pub(super) struct NtfsLazyScanState {
    /// File offset in the SpanReader where the next compressed block
    /// should be read from.
    pub(super) file_offset: u64,
    /// Decompressed stream offset corresponding to `file_offset`.
    pub(super) decomp_offset: u64,
    /// Total file size (end of scan region).
    pub(super) file_size: u64,
    /// VBR for inline MFT record parsing.
    pub(super) vbr: fs_pc::ntfs::NtfsVbr,
    /// Cluster size (bytes).
    pub(super) cluster_size: u64,
    /// Pending inline-MFT data runs not yet matched to run headers.
    pub(super) pending_lcns: Vec<(u64, u64)>,
    /// Index into `pending_lcns` where the most recently parsed gap's entries
    /// begin. Stream runs are matched against this freshest group first (in
    /// forward order); see `match_inline_run`.
    pub(super) pending_group_start: usize,
    /// Data-run queue from the MFT (Phase 1), consumed during matching.
    pub(super) data_run_queue: Vec<(u64, u64)>,
    /// Absolute decompressed offset where the previous run's data ends.
    pub(super) prev_data_end: u64,
    /// Rolling decompressed stream buffer for gap/header parsing.
    pub(super) stream: Vec<u8>,
    /// Base offset of `stream[0]` in the decompressed address space.
    pub(super) stream_base: u64,
    /// Remaining decompressed bytes of cluster data to skip before
    /// the next gap/run-header region.
    pub(super) skip_data_bytes: u64,
    /// Decompressed offset up to which we have already searched for run
    /// headers and found none. Avoids re-scanning the same bytes on every
    /// subsequent block (which would be O(N^2)). Always >= `prev_data_end`.
    pub(super) search_resume: u64,
    /// LCN starts already assigned to a run. Ghost re-declares some files'
    /// runs both inline and in the MFT queue, so a stream run can otherwise
    /// match an already-used `(lcn, cc)` and land two files on the same
    /// clusters -> cross-links in chkdsk. Used to skip already-assigned LCNs.
    pub(super) assigned_lcns: std::collections::HashSet<u64>,
}

// ---------------------------------------------------------------------------
// NTFS file-aware lazy-scan cache
//
// Building the run index for a compressed NTFS file-aware GHO means scanning
// the whole compressed stream forward (decompressing every block) to find each
// file's cluster runs — `extend_ntfs_compressed_index`. The GUI opens a fresh
// `GhoReader` for nearly every browse/extract operation (see the MEMORY.md note
// about not caching state on the filesystem struct), so without a cache that
// full scan is paid again on each extract. This process-wide cache stores the
// completed scan keyed by the archive's file identity and reuses it across
// reader instances.
// ---------------------------------------------------------------------------

/// Identity of a GHO span set for the lazy-scan cache: each span file's path,
/// length, and modification time. A change to any span file (re-backup,
/// truncation) yields a different key, so a stale scan is never reused.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct NtfsScanKey(pub(super) Vec<(PathBuf, u64, u128)>);

impl NtfsScanKey {
    /// Build a key from the span set, or `None` if any file's metadata
    /// (length / mtime) can't be read — in which case caching is skipped.
    pub(super) fn from_span_set(span_set: &[PathBuf]) -> Option<Self> {
        let mut parts = Vec::with_capacity(span_set.len());
        for p in span_set {
            let meta = std::fs::metadata(p).ok()?;
            let mtime = meta
                .modified()
                .ok()?
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_nanos();
            parts.push((p.clone(), meta.len(), mtime));
        }
        Some(NtfsScanKey(parts))
    }
}

/// A completed NTFS file-aware lazy scan, cached in memory so re-opening the
/// same archive reuses the run index + decompressed-block table instead of
/// re-scanning the whole compressed stream.
pub(super) struct CachedNtfsScan {
    pub(super) runs: Vec<NtfsGhoClusterRun>,
    pub(super) blocks: Vec<NtfsCompressedBlock>,
    pub(super) compression: GhoCompression,
    pub(super) total_decompressed: u64,
    pub(super) volume_size: u64,
    pub(super) cluster_size: u64,
    pub(super) vbr: [u8; 512],
    pub(super) bitmap_runs: Vec<(u64, u64)>,
    pub(super) synth_bitmap: Vec<u8>,
    pub(super) boot_code: Vec<u8>,
}

/// Max archives kept in the scan cache. Each entry can hold a few MB of run /
/// block tables, and users rarely juggle more than one or two GHOs at once;
/// eviction is FIFO.
pub(super) const NTFS_SCAN_CACHE_CAP: usize = 4;

#[derive(Default)]
pub(super) struct NtfsScanCacheInner {
    pub(super) map: std::collections::HashMap<NtfsScanKey, Arc<CachedNtfsScan>>,
    /// FIFO insertion order for eviction.
    pub(super) order: std::collections::VecDeque<NtfsScanKey>,
}

pub(super) fn ntfs_scan_cache() -> &'static Mutex<NtfsScanCacheInner> {
    static CACHE: OnceLock<Mutex<NtfsScanCacheInner>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(NtfsScanCacheInner::default()))
}

pub(super) fn ntfs_scan_cache_get(key: &NtfsScanKey) -> Option<Arc<CachedNtfsScan>> {
    let cache = ntfs_scan_cache().lock().ok()?;
    cache.map.get(key).cloned()
}

/// Snapshot the (now fully-scanned) index + compressed state and store it under
/// `key`, replacing any prior entry. Cheap to call repeatedly: later calls just
/// refresh the entry as `$Bitmap` / `$Boot` get filled in.
pub(super) fn ntfs_scan_cache_store(
    key: &NtfsScanKey,
    index: &NtfsGhoIndex,
    comp: &NtfsCompressedState,
) {
    let snap = Arc::new(CachedNtfsScan {
        runs: index.runs.clone(),
        blocks: comp.blocks.clone(),
        compression: comp.compression,
        total_decompressed: comp.total_decompressed,
        volume_size: index.volume_size,
        cluster_size: index.cluster_size,
        vbr: index.vbr,
        bitmap_runs: index.bitmap_runs.clone(),
        synth_bitmap: index.synth_bitmap.clone(),
        boot_code: index.boot_code.clone(),
    });
    let Ok(mut cache) = ntfs_scan_cache().lock() else {
        return;
    };
    if cache.map.insert(key.clone(), snap).is_none() {
        // New key: record order and evict the oldest if over capacity.
        cache.order.push_back(key.clone());
        while cache.order.len() > NTFS_SCAN_CACHE_CAP {
            if let Some(old) = cache.order.pop_front() {
                cache.map.remove(&old);
            }
        }
    }
    log::info!(
        "Cached NTFS lazy-scan result: {} runs, {} blocks ({} archive(s) cached)",
        index.runs.len(),
        comp.blocks.len(),
        cache.map.len(),
    );
}

/// Reconstruct a fully-scanned `NtfsFileAware` reader mode from a cached
/// snapshot, skipping the MFT decompression and the forward scan entirely.
pub(super) fn ntfs_mode_from_cache(
    snap: &CachedNtfsScan,
    key: NtfsScanKey,
) -> (GhoReaderMode, u64) {
    let index = NtfsGhoIndex {
        runs: snap.runs.clone(),
        volume_size: snap.volume_size,
        cluster_size: snap.cluster_size,
        vbr: snap.vbr,
        bitmap_runs: snap.bitmap_runs.clone(),
        synth_bitmap: snap.synth_bitmap.clone(),
        boot_code: snap.boot_code.clone(),
    };
    let comp = NtfsCompressedState {
        blocks: snap.blocks.clone(),
        compression: snap.compression,
        total_decompressed: snap.total_decompressed,
        cache_block: None,
        cache_buf: Vec::new(),
        lazy_scan: None,
        cache_key: Some(key),
    };
    let served_size = ntfs_served_size(&index);
    (
        GhoReaderMode::NtfsFileAware {
            index,
            last_run_hint: 0,
            compressed: Some(comp),
        },
        served_size,
    )
}

/// 16-byte constant prefix appearing at the start of an NTFS file-aware
/// MFT header (the first run only). Subsequent runs use [`NTFS_RUN_ENTRY_B`]
/// directly without this entry A prefix.
pub(super) const NTFS_RUN_HEADER_ENTRY1: [u8; 16] = [
    0x0E, 0x20, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,
];

/// Run-header entry B layout (16 bytes):
///   byte 0:      0x0E (entry marker)
///   bytes 1-6:   0x02 0x00 0x00 0x00 0x00 0x00 (fixed)
///   bytes 7-14:  u64-LE run sequence number (0 = $MFT, 1+ subsequent)
///   byte 15:     0x0F (terminator)
/// Only the fixed bytes (0-6, 15) are matched; the seq field is read out
/// separately by `parse_ntfs_run_b_header`.
pub(super) fn match_run_entry_b(window: &[u8]) -> bool {
    if window.len() < 16 {
        return false;
    }
    window[0] == 0x0E
        && window[1] == 0x02
        && window[2] == 0x00
        && window[3] == 0x00
        && window[4] == 0x00
        && window[5] == 0x00
        && window[6] == 0x00
        && window[15] == 0x0F
}

/// Size of the 3-entry MFT header (entry A + entry B + entry C).
pub(super) const NTFS_RUN_HEADER_SIZE: usize = 42;

/// Size of a non-MFT run header (entry B + entry C only).
pub(super) const NTFS_RUN_B_HEADER_SIZE: usize = 26;

/// One cluster run in the NTFS file-aware index: maps a contiguous range
/// of LCNs to a file offset in the GHO.
#[derive(Debug, Clone)]
pub(super) struct NtfsGhoClusterRun {
    pub(super) lcn_start: u64,
    pub(super) cluster_count: u64,
    pub(super) file_offset: u64,
    /// Run header bytes 7-14 carry a u64 sequence number: the fragment index
    /// within an attribute (0 = first fragment of a new attribute; 1+ =
    /// continuation). Single-fragment attributes are always seq=0. Captured
    /// during the lazy scan and used by [`fixup_ntfs_typed_misalignments`] to
    /// deterministically map stream runs to MFT attributes — when an
    /// attribute has multiple fragments, the seq chain uniquely identifies
    /// the attribute (the cc sequence is unique among multi-fragment attrs).
    /// `u64::MAX` for entries that pre-date the seq capture (e.g. the
    /// explicitly-registered MFT first run added at scan start).
    pub(super) seq: u64,
}

/// Index built during the open scan. Sorted by `lcn_start` for binary
/// search during reads.
pub(super) struct NtfsGhoIndex {
    pub(super) runs: Vec<NtfsGhoClusterRun>,
    pub(super) volume_size: u64,
    pub(super) cluster_size: u64,
    #[allow(dead_code)]
    pub(super) vbr: [u8; 512],
    /// Synthesized `$Bitmap`. Ghost omits `$Bitmap` content from file-aware
    /// backups (it's derivable from the MFT allocation and Ghost's restorer
    /// regenerates it), so the whole-disk stream must serve a synthesized one
    /// or a strict NTFS driver sees an all-free/corrupt volume and won't mount.
    /// `bitmap_runs` are `$Bitmap`'s own `$DATA` LCN runs (where to serve it);
    /// `synth_bitmap` is one bit per cluster, LSB-first, set = allocated.
    pub(super) bitmap_runs: Vec<(u64, u64)>,
    pub(super) synth_bitmap: Vec<u8>,
    /// `$Boot`'s first 16 sectors (8 KiB), recovered from the stream by
    /// content-matching the VBR (the read path otherwise synthesizes only the
    /// VBR for the boot region and zeros sectors 1-15). Empty until
    /// `prepare_full_image` runs (export only) or if no match is found; the read
    /// path then falls back to VBR + zeros. Serving the real boot code (sectors
    /// 1-15) is what makes the exported disk bootable.
    pub(super) boot_code: Vec<u8>,
}

/// Build a synthesized NTFS `$Bitmap` from the MFT-declared allocation.
///
/// `allocation` is every `(lcn, cluster_count)` the MFT marks in use (the
/// `data_run_queue` plus the `$MFT` run itself). Returns the bitmap bytes sized
/// to cover the whole volume (`ceil(total_clusters / 8)`), one bit per cluster,
/// LSB-first, set = allocated. Bits for clusters past the volume end (padding in
/// the final byte) are set to 1 per NTFS convention.
pub(super) fn build_synth_ntfs_bitmap(allocation: &[(u64, u64)], total_clusters: u64) -> Vec<u8> {
    let nbytes = (total_clusters as usize).div_ceil(8);
    let mut bm = vec![0u8; nbytes];
    for &(lcn, len) in allocation {
        let end = lcn.saturating_add(len).min(total_clusters);
        for c in lcn..end {
            bm[(c / 8) as usize] |= 1 << (c % 8);
        }
    }
    // Clusters past the volume end occupy the slack bits of the final byte;
    // NTFS marks them allocated so nothing tries to use them.
    for c in total_clusters..(nbytes as u64 * 8) {
        bm[(c / 8) as usize] |= 1 << (c % 8);
    }
    bm
}
/// Parse inline MFT records from a buffer (gap between cluster runs).
/// Same logic as `parse_inline_mft_records` but works on an in-memory
/// slice instead of a reader.
pub(super) fn parse_inline_mft_from_buf(
    buf: &[u8],
    vbr: &fs_pc::ntfs::NtfsVbr,
    out: &mut Vec<(u64, u64)>,
) {
    use fs_pc::ntfs::{apply_fixup, parse_mft_attributes};
    let rec_size = vbr.mft_record_size as usize;
    if buf.len() < rec_size + 4 {
        return;
    }
    let mut pos = 0;
    while pos + rec_size <= buf.len() {
        if let Some(idx) = buf[pos..].windows(4).position(|w| w == b"FILE") {
            let rec_off = pos + idx;
            if rec_off + rec_size > buf.len() {
                break;
            }
            let rec_num = u32::from_le_bytes([
                buf[rec_off + 44],
                buf[rec_off + 45],
                buf[rec_off + 46],
                buf[rec_off + 47],
            ]);
            if rec_num == 8 {
                pos = rec_off + rec_size;
                continue;
            }
            let mut rec = buf[rec_off..rec_off + rec_size].to_vec();
            let _ = apply_fixup(&mut rec);
            let attrs = parse_mft_attributes(&rec, vbr.mft_record_size);
            for attr in &attrs {
                if !attr.resident && !attr.data_runs.is_empty() {
                    for dr in &attr.data_runs {
                        // Skip sparse runs. decode_data_runs returns
                        // cluster_offset == 0 for holes (offset_size == 0); they
                        // have no stored content and no real LCN. Queuing them as
                        // (lcn=0, cc) makes real stream runs match LCN 0 and pile
                        // up on $Boot's clusters -> cross-links in chkdsk.
                        if dr.cluster_offset > 0 && dr.length > 0 {
                            out.push((dr.cluster_offset as u64, dr.length));
                        }
                    }
                }
            }
            pos = rec_off + rec_size;
        } else {
            break;
        }
    }
}

/// A `Read + Seek` adapter over a compressed NTFS block stream. Used
/// for on-demand reads after the index has been built.
pub(super) struct NtfsDecompressingReader<'a, R> {
    pub(super) inner: &'a mut R,
    pub(super) state: &'a mut NtfsCompressedState,
    pub(super) position: u64,
}

impl<'a, R: Read + Seek> NtfsDecompressingReader<'a, R> {
    pub(super) fn ensure_block_cached(&mut self, idx: usize) -> std::io::Result<()> {
        if self.state.cache_block == Some(idx) {
            return Ok(());
        }
        let block = &self.state.blocks[idx];
        let comp_size = block.comp_size as usize;
        let mut comp_buf = vec![0u8; comp_size];
        self.inner
            .seek(SeekFrom::Start(block.file_offset))
            .map_err(disk_core::io_other)?;
        self.inner
            .read_exact(&mut comp_buf)
            .map_err(disk_core::io_other)?;

        self.state.cache_buf.clear();
        self.state
            .cache_buf
            .resize(block.decomp_size as usize + 512, 0);

        let n = match self.state.compression {
            GhoCompression::High => {
                use flate2::read::ZlibDecoder;
                let mut dec = ZlibDecoder::new(&comp_buf[..]);
                let mut total = 0;
                loop {
                    match dec.read(&mut self.state.cache_buf[total..]) {
                        Ok(0) => break,
                        Ok(k) => total += k,
                        Err(e) => return Err(e),
                    }
                }
                total
            }
            GhoCompression::Fast => {
                fast_lz_decompress(&comp_buf, comp_size, &mut self.state.cache_buf)
                    .map_err(|e| disk_core::io_other(format!("{e:#}")))?
            }
            _ => {
                return Err(disk_core::io_other("unsupported compression"));
            }
        };
        self.state.cache_buf.truncate(n);
        self.state.cache_block = Some(idx);
        Ok(())
    }

    pub(super) fn find_block(&self, pos: u64) -> Option<usize> {
        self.state
            .blocks
            .binary_search_by(|b| {
                if pos < b.decomp_offset {
                    std::cmp::Ordering::Greater
                } else if pos >= b.decomp_offset + b.decomp_size as u64 {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
    }
}

impl<R: Read + Seek> Read for NtfsDecompressingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.position >= self.state.total_decompressed {
            return Ok(0);
        }
        let Some(idx) = self.find_block(self.position) else {
            return Ok(0);
        };
        self.ensure_block_cached(idx)?;
        let block = &self.state.blocks[idx];
        let off_in_block = (self.position - block.decomp_offset) as usize;
        let avail = self.state.cache_buf.len().saturating_sub(off_in_block);
        let n = avail.min(buf.len());
        buf[..n].copy_from_slice(&self.state.cache_buf[off_in_block..off_in_block + n]);
        self.position += n as u64;
        Ok(n)
    }
}

impl<R: Read + Seek> Seek for NtfsDecompressingReader<'_, R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p,
            SeekFrom::Current(delta) => {
                if delta >= 0 {
                    self.position.saturating_add(delta as u64)
                } else {
                    self.position.saturating_sub((-delta) as u64)
                }
            }
            SeekFrom::End(delta) => {
                let total = self.state.total_decompressed;
                if delta >= 0 {
                    total.saturating_add(delta as u64)
                } else {
                    total.saturating_sub((-delta) as u64)
                }
            }
        };
        self.position = new_pos;
        Ok(new_pos)
    }
}

/// Locate the NTFS VBR embedded in the GHPR metadata region. Scans from
/// `start` to `end` for the 3-byte jump + `NTFS    ` OEM ID + valid
/// `0x55 0xAA` boot signature.
pub(super) fn find_ntfs_vbr_in_header<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
) -> Result<(u64, [u8; 512])> {
    let mut buf = [0u8; 512];
    let mut off = start;
    while off + 512 <= end {
        reader.seek(SeekFrom::Start(off))?;
        if reader.read_exact(&mut buf).is_err() {
            break;
        }
        if &buf[3..11] == b"NTFS    " && buf[510] == 0x55 && buf[511] == 0xAA {
            return Ok((off, buf));
        }
        off += 1;
    }
    bail!("NTFS VBR not found in GHPR metadata region");
}

/// Parse the 42-byte MFT run header. Returns the cluster count, or `None`
/// if the header doesn't match the MFT pattern (entry A + entry B + entry C).
pub(super) fn parse_ntfs_run_header(data: &[u8]) -> Option<u64> {
    if data.len() < NTFS_RUN_HEADER_SIZE {
        return None;
    }
    if data[0..16] != NTFS_RUN_HEADER_ENTRY1 {
        return None;
    }
    if data[32] != 0x0F || data[41] != 0x0E {
        return None;
    }
    let cluster_count = u64::from_le_bytes([
        data[33], data[34], data[35], data[36], data[37], data[38], data[39], data[40],
    ]);
    Some(cluster_count)
}

/// Map an absolute decompressed offset to a position within the rolling
/// `scan.stream` buffer (whose first byte is at `stream_base`). Returns
/// `None` if the offset is before the buffer or past its end. Free function
/// (not a closure) so it doesn't borrow `scan` across mutations.
pub(super) fn stream_local_offset(abs: u64, stream_base: u64, stream_len: usize) -> Option<usize> {
    if abs >= stream_base {
        let l = (abs - stream_base) as usize;
        if l <= stream_len {
            return Some(l);
        }
    }
    None
}

/// Parse the 26-byte run header used for every non-MFT data run
/// (entry B + entry C). Returns `(sequence_number, cluster_count)`, or
/// `None` if the header doesn't match. The sequence number (entry B bytes
/// 7-14, u64-LE) is the run's index in MFT data-run order: 0 = $MFT, 1 =
/// first non-MFT run, etc. — used to match the run to its LCN exactly.
pub(super) fn parse_ntfs_run_b_header(data: &[u8]) -> Option<(u64, u64)> {
    if data.len() < NTFS_RUN_B_HEADER_SIZE {
        return None;
    }
    if !match_run_entry_b(&data[0..16]) {
        return None;
    }
    if data[16] != 0x0F || data[25] != 0x0E {
        return None;
    }
    let seq = u64::from_le_bytes([
        data[7], data[8], data[9], data[10], data[11], data[12], data[13], data[14],
    ]);
    let cluster_count = u64::from_le_bytes([
        data[17], data[18], data[19], data[20], data[21], data[22], data[23], data[24],
    ]);
    Some((seq, cluster_count))
}

/// Parse inline MFT FILE records in the gap between cluster runs.
/// Appends `(lcn, cluster_count)` entries to `out` for every non-resident
/// attribute that has data runs (`$DATA`, `$INDEX_ALLOCATION`, `$BITMAP`,
/// `$SECURITY_DESCRIPTOR`, etc.). Ghost stores cluster data for ALL
/// non-resident attributes, not just `$DATA`.
pub(super) fn parse_inline_mft_records<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
    vbr: &fs_pc::ntfs::NtfsVbr,
    out: &mut Vec<(u64, u64)>,
) {
    use fs_pc::ntfs::{apply_fixup, parse_mft_attributes};
    let rec_size = vbr.mft_record_size as usize;
    if end < start || ((end - start) as usize) < rec_size + 4 {
        return;
    }
    // A 4 MiB window slides over the gap so a large gap is never read whole.
    const WINDOW: u64 = 4 * 1024 * 1024;
    let window = WINDOW.max(2 * rec_size as u64);
    let mut base = start;
    let mut buf = Vec::new();
    let fill = |reader: &mut R, at: u64, buf: &mut Vec<u8>| -> bool {
        buf.resize((end - at).min(window) as usize, 0);
        if reader.seek(SeekFrom::Start(at)).is_err() {
            return false;
        }
        match read_fully_or_eof(reader, buf) {
            Ok(n) => {
                buf.truncate(n);
                true
            }
            Err(_) => false,
        }
    };
    if !fill(reader, base, &mut buf) {
        return;
    }
    let mut pos = 0;
    loop {
        // The window reaches the gap's end (or the file's) when it came back short of `window`.
        let at_end = (buf.len() as u64) < window || base + buf.len() as u64 >= end;
        if pos + rec_size > buf.len() && at_end {
            break;
        }
        match buf[pos..].windows(4).position(|w| w == b"FILE") {
            Some(idx) if pos + idx + rec_size <= buf.len() => {
                let rec_off = pos + idx;
                let mut rec = buf[rec_off..rec_off + rec_size].to_vec();
                pos = rec_off + rec_size;
                let rec_num = u32::from_le_bytes([rec[44], rec[45], rec[46], rec[47]]);
                // Skip $BadClus (record 8) — sparse run covering entire volume.
                if rec_num == 8 {
                    continue;
                }
                let _ = apply_fixup(&mut rec);
                let attrs = parse_mft_attributes(&rec, vbr.mft_record_size);
                for attr in &attrs {
                    if !attr.resident && !attr.data_runs.is_empty() {
                        for dr in &attr.data_runs {
                            // Skip sparse runs. decode_data_runs returns
                            // cluster_offset == 0 for holes (offset_size == 0); they
                            // have no stored content and no real LCN. Queuing them as
                            // (lcn=0, cc) makes real stream runs match LCN 0 and pile
                            // up on $Boot's clusters -> cross-links in chkdsk.
                            if dr.cluster_offset > 0 && dr.length > 0 {
                                out.push((dr.cluster_offset as u64, dr.length));
                            }
                        }
                    }
                }
            }
            // A record cut by the window's end: slide the window to start at it.
            Some(idx) if !at_end => {
                base += (pos + idx) as u64;
                pos = 0;
                if !fill(reader, base, &mut buf) {
                    return;
                }
            }
            // No signature left in this window: slide on, keeping 3 bytes for a split "FILE".
            None if !at_end => {
                base += buf.len().saturating_sub(3).max(1) as u64;
                pos = 0;
                if !fill(reader, base, &mut buf) {
                    return;
                }
            }
            _ => break,
        }
    }
}

/// Build the NTFS cluster run index by walking the packed data stream.
///
/// `(cluster bytes, volume bytes)` from an NTFS boot sector, refusing values no NTFS volume
/// has, since the volume size sizes allocations (the synthesized `$Bitmap`) downstream.
pub(super) fn ntfs_vbr_geometry(vbr: &fs_pc::ntfs::NtfsVbr) -> Result<(u64, u64)> {
    let (bps, spc) = (vbr.bytes_per_sector, vbr.sectors_per_cluster);
    if !(512..=4096).contains(&bps) || !bps.is_power_of_two() || spc == 0 || spc > 4096 {
        bail!("NTFS boot sector: {bps}-byte sectors, {spc} per cluster");
    }
    let cluster = bps * spc;
    let volume = vbr
        .total_sectors
        .checked_mul(bps)
        .filter(|v| v / cluster <= u32::MAX as u64)
        .ok_or_else(|| {
            anyhow!(
                "NTFS boot sector: {} sectors is too many",
                vbr.total_sectors
            )
        })?;
    if !(256..=65536).contains(&vbr.mft_record_size) {
        bail!("NTFS boot sector: MFT record size {}", vbr.mft_record_size);
    }
    Ok((cluster, volume))
}

/// Strategy: the first run is always the MFT. We read it, parse every
/// MFT record to collect all non-resident `$DATA` attribute data runs,
/// and build a queue of `(LCN, cluster_count)` entries sorted by MFT
/// record number. Each subsequent cluster run in the file consumes the
/// next entry from this queue (matched by cluster count).
pub(super) fn index_ntfs_file_aware<R: Read + Seek>(
    reader: &mut R,
    file_size: u64,
    vbr: &fs_pc::ntfs::NtfsVbr,
    vbr_raw: &[u8; 512],
) -> Result<NtfsGhoIndex> {
    use fs_pc::ntfs::{apply_fixup, parse_mft_attributes};

    let (cluster_size, volume_size) = ntfs_vbr_geometry(vbr)?;
    let mft_record_size = vbr.mft_record_size as usize;

    // Scan for the first run header. For raw files the GHPR metadata
    // occupies the first ~8 KiB, so we skip it; for the decompressed
    // view of a compressed file the stream starts right at the data.
    let scan_start = 0u64;
    let scan_end = file_size.min(scan_start + 256 * 1024);
    let mut header_buf = [0u8; NTFS_RUN_HEADER_SIZE];
    let mut first_run_offset: Option<u64> = None;

    let mut off = scan_start;
    while off + NTFS_RUN_HEADER_SIZE as u64 <= scan_end {
        reader.seek(SeekFrom::Start(off))?;
        if reader.read_exact(&mut header_buf).is_err() {
            break;
        }
        if parse_ntfs_run_header(&header_buf).is_some() {
            first_run_offset = Some(off);
            break;
        }
        off += 1;
    }
    let first_run = first_run_offset
        .ok_or_else(|| anyhow!("NTFS file-aware: no cluster run header found in scan region"))?;

    // --- Phase 1: read the MFT (run 0) ---
    reader.seek(SeekFrom::Start(first_run))?;
    reader.read_exact(&mut header_buf)?;
    let mft_clusters = parse_ntfs_run_header(&header_buf)
        .ok_or_else(|| anyhow!("first run header is not valid"))?;
    let mft_data_start = first_run + NTFS_RUN_HEADER_SIZE as u64;
    let mft_size = mft_clusters * cluster_size;

    log::info!(
        "Reading MFT: {} clusters ({:.1} MB) from offset {:#x}",
        mft_clusters,
        mft_size as f64 / (1024.0 * 1024.0),
        mft_data_start
    );

    // Parse all MFT records to build the data-run queue.
    // Each entry: (mft_record_number, lcn, cluster_count)
    let mut data_run_queue: Vec<(u64, u64)> = Vec::new();
    // $Bitmap (record 6) $DATA runs — where the synthesized bitmap is served.
    let mut bitmap_runs: Vec<(u64, u64)> = Vec::new();
    // $MFTMirr (record 1) $DATA run — (lcn, cluster_count). Ghost omits the
    // mirror's content from the stream (it's a verbatim copy of $MFT's first
    // records, hence derivable), so we synthesize it by mapping these clusters
    // back to the start of $MFT's data. See the run injection below.
    let mut mftmirr_run: Option<(u64, u64)> = None;
    let num_records = mft_size / mft_record_size as u64;
    let mut rec_buf = vec![0u8; mft_record_size];

    for rec_idx in 0..num_records {
        let rec_off = mft_data_start + rec_idx * mft_record_size as u64;
        reader.seek(SeekFrom::Start(rec_off))?;
        if reader.read_exact(&mut rec_buf).is_err() {
            break;
        }
        if &rec_buf[0..4] != b"FILE" {
            continue;
        }
        let rec_num = u32::from_le_bytes([rec_buf[44], rec_buf[45], rec_buf[46], rec_buf[47]]);
        // Skip $BadClus (record 8) — it has a sparse data run covering
        // the entire volume but no actual data is stored.
        if rec_num == 8 {
            continue;
        }
        let _ = apply_fixup(&mut rec_buf);
        let attrs = parse_mft_attributes(&rec_buf, vbr.mft_record_size);
        for attr in &attrs {
            if attr.resident || attr.data_runs.is_empty() {
                continue;
            }
            // Record 0 is $MFT itself; fragment 0 is already registered as
            // run 0, but its later $DATA fragments must be queued so they
            // get mapped — otherwise high-numbered MFT records read as zeros.
            let skip_first = rec_num == 0 && attr.attr_type == 0x80;
            for (i, dr) in attr.data_runs.iter().enumerate() {
                if skip_first && i == 0 {
                    continue;
                }
                // Skip sparse runs (cluster_offset == 0 = hole); see the inline
                // MFT parser for why queuing them as lcn=0 causes cross-links.
                if dr.cluster_offset > 0 && dr.length > 0 {
                    // $MFTMirr's $DATA: remember it for synthesis, don't queue
                    // it (no stream run will ever match — Ghost omits the
                    // mirror content). Queuing would just leave it unmapped.
                    if rec_num == 1 && attr.attr_type == 0x80 && mftmirr_run.is_none() {
                        mftmirr_run = Some((dr.cluster_offset as u64, dr.length));
                        continue;
                    }
                    data_run_queue.push((dr.cluster_offset as u64, dr.length));
                    if rec_num == 6 && attr.attr_type == 0x80 {
                        bitmap_runs.push((dr.cluster_offset as u64, dr.length));
                    }
                }
            }
        }
    }

    log::info!(
        "MFT parsed: {} data-run entries from {} MFT records",
        data_run_queue.len(),
        num_records
    );

    // --- Phase 2: walk all cluster runs using the run-header needle ---
    let mut runs: Vec<NtfsGhoClusterRun> = Vec::new();
    let mut total_stored_clusters: u64 = 0;

    // Add the MFT as run 0. seq=0 — it's the first fragment of $MFT's $DATA.
    runs.push(NtfsGhoClusterRun {
        lcn_start: vbr.mft_cluster,
        cluster_count: mft_clusters,
        file_offset: mft_data_start,
        seq: 0,
    });
    total_stored_clusters += mft_clusters;

    // Synthesize $MFTMirr. NTFS keeps $MFTMirr as a byte-verbatim copy of
    // $MFT's first records (the mirror cluster equals the MFT's first cluster,
    // confirmed against ground-truth disks), so map the mirror's clusters
    // straight back to the start of $MFT's stream data. Ghost stores no mirror
    // content of its own (file-aware backups omit derivable system streams the
    // way they omit $Bitmap / $Boot), so without this the mirror reads as zeros
    // and chkdsk reports "$MFTMirr ($DATA) is corrupt / cross-linked".
    if let Some((mirr_lcn, mirr_cc)) = mftmirr_run {
        let cc = mirr_cc.min(mft_clusters);
        if cc > 0 {
            runs.push(NtfsGhoClusterRun {
                lcn_start: mirr_lcn,
                cluster_count: cc,
                file_offset: mft_data_start,
                seq: 0,
            });
            log::info!(
                "Synthesized $MFTMirr: {} cluster(s) at lcn {} mapped to $MFT data",
                cc,
                mirr_lcn
            );
        }
    }

    // Pending data runs from inline MFT records. Matched to cluster runs in
    // MFT-walk order, freshest gap first (see match_inline_run).
    let mut pending_lcns: Vec<(u64, u64)> = Vec::new();
    let mut pending_group_start: usize = 0;

    // LCN starts already assigned — prevents two stream runs landing on the
    // same clusters when Ghost re-declares a run inline and in the MFT queue.
    let mut assigned: std::collections::HashSet<u64> = runs.iter().map(|r| r.lcn_start).collect();

    // Runs that couldn't be matched to an inline MFT record's data runs.
    // Resolved via $Bitmap after the main scan.
    let mut unmapped_runs: Vec<(u64, u64)> = Vec::new(); // (cluster_count, file_offset)

    // Scan the entire file for run headers using the 16-byte needle.
    let needle = &NTFS_RUN_HEADER_ENTRY1;
    let chunk_size: usize = 4 * 1024 * 1024;
    let mut search_pos = mft_data_start + mft_size;
    let mut prev_data_end = search_pos;

    while search_pos < file_size {
        let read_len = ((file_size - search_pos) as usize).min(chunk_size + 64);
        let mut chunk = vec![0u8; read_len];
        reader.seek(SeekFrom::Start(search_pos))?;
        let n = read_fully_or_eof(reader, &mut chunk)?;
        chunk.truncate(n);

        let mut scan_pos = 0;
        while scan_pos + NTFS_RUN_HEADER_SIZE <= chunk.len() {
            if let Some(idx) = chunk[scan_pos..].windows(16).position(|w| w == needle) {
                let abs_off = search_pos + (scan_pos + idx) as u64;
                let local_off = scan_pos + idx;
                if local_off + NTFS_RUN_HEADER_SIZE <= chunk.len() {
                    if let Some(cc) = parse_ntfs_run_header(&chunk[local_off..]) {
                        if cc > 0 {
                            let data_start = abs_off + NTFS_RUN_HEADER_SIZE as u64;
                            let data_end = data_start + cc * cluster_size;
                            if data_end > file_size + cluster_size {
                                scan_pos = chunk.len();
                                break;
                            }

                            // Parse inline MFT records in the gap since the
                            // last run. Each gap may contain multiple FILE
                            // records whose data runs describe subsequent runs.
                            // Append to (not replace) pending_lcns — a single
                            // gap's MFT records can provide LCNs for many
                            // subsequent runs.
                            let gap_start = prev_data_end;
                            let gap_end = abs_off;
                            if gap_end > gap_start && (gap_end - gap_start) < 1024 * 1024 {
                                let len_before = pending_lcns.len();
                                parse_inline_mft_records(
                                    reader,
                                    gap_start,
                                    gap_end,
                                    vbr,
                                    &mut pending_lcns,
                                );
                                // A non-empty gap starts a fresh group: the runs
                                // that follow consume these entries in MFT order.
                                if pending_lcns.len() > len_before {
                                    pending_group_start = len_before;
                                }
                            }

                            // Determine LCN from pending data runs in MFT-walk
                            // order (freshest gap first). Skip already-assigned
                            // LCNs so a re-declared run doesn't double-map onto
                            // another file's clusters.
                            let lcn = match_inline_run(
                                &mut pending_lcns,
                                &mut pending_group_start,
                                cc,
                                &assigned,
                            );

                            if let Some(lcn) = lcn {
                                assigned.insert(lcn);
                                runs.push(NtfsGhoClusterRun {
                                    lcn_start: lcn,
                                    cluster_count: cc,
                                    file_offset: data_start,
                                    // TODO: parse seq from entry B inside the 3-entry header
                                    // for the uncompressed path. Currently a sentinel — fixup
                                    // will fall back to non-seq matching for these.
                                    seq: u64::MAX,
                                });
                            } else {
                                unmapped_runs.push((cc, data_start));
                            }
                            total_stored_clusters += cc;
                            prev_data_end = data_end;

                            if data_end > search_pos + chunk.len() as u64 {
                                search_pos = data_end;
                                scan_pos = chunk.len();
                                break;
                            } else {
                                scan_pos = (data_end - search_pos) as usize;
                                continue;
                            }
                        }
                    }
                }
                scan_pos += idx + 16;
            } else {
                break;
            }
        }

        if scan_pos < chunk.len() {
            search_pos += chunk.len().saturating_sub(15) as u64;
        } else if search_pos + chunk.len() as u64 >= file_size {
            break;
        }
    }

    // --- Phase 3: resolve unmapped runs via $Bitmap ---
    if !unmapped_runs.is_empty() {
        log::info!(
            "{} unmapped runs ({} clusters), resolving via MFT data-run queue",
            unmapped_runs.len(),
            unmapped_runs.iter().map(|(cc, _)| cc).sum::<u64>()
        );

        // Build a set of already-mapped LCN ranges for fast lookup.
        let mut mapped_lcns: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for run in &runs {
            for c in 0..run.cluster_count {
                mapped_lcns.insert(run.lcn_start + c);
            }
        }

        // Collect unmapped data runs from the MFT queue (those not
        // consumed by inline matching). Match by cluster count.
        let mut remaining_queue: Vec<(u64, u64)> = data_run_queue
            .iter()
            .filter(|&&(lcn, len)| {
                // Exclude runs already mapped.
                !(0..len).any(|c| mapped_lcns.contains(&(lcn + c)))
            })
            .copied()
            .collect();

        let mut resolved = 0u64;
        for &(cc, file_offset) in &unmapped_runs {
            if let Some(lcn) = pop_unassigned_lcn(&mut remaining_queue, cc, &assigned) {
                assigned.insert(lcn);
                runs.push(NtfsGhoClusterRun {
                    lcn_start: lcn,
                    cluster_count: cc,
                    file_offset,
                    seq: u64::MAX,
                });
                resolved += 1;
            }
        }

        log::info!(
            "Resolved {} of {} unmapped runs via MFT queue fallback",
            resolved,
            unmapped_runs.len()
        );
    }

    // Sort by LCN for binary search during reads.
    runs.sort_by_key(|r| r.lcn_start);

    log::info!(
        "NTFS file-aware index: {} runs, {} stored clusters ({:.1} MB), volume {:.1} GB",
        runs.len(),
        total_stored_clusters,
        total_stored_clusters as f64 * cluster_size as f64 / (1024.0 * 1024.0),
        volume_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    // Synthesize $Bitmap from the complete run set (every allocated cluster
    // with stored content) plus $Bitmap's own clusters. The uncompressed
    // builder discovers all runs here, so the bitmap is complete now.
    let total_clusters = volume_size / cluster_size;
    let mut allocation: Vec<(u64, u64)> = runs
        .iter()
        .map(|r| (r.lcn_start, r.cluster_count))
        .collect();
    allocation.extend_from_slice(&bitmap_runs);
    let synth_bitmap = build_synth_ntfs_bitmap(&allocation, total_clusters);
    log::info!(
        "Synthesized $Bitmap: {} bytes over {} run(s) for {} clusters",
        synth_bitmap.len(),
        bitmap_runs.len(),
        total_clusters,
    );

    Ok(NtfsGhoIndex {
        runs,
        volume_size,
        cluster_size,
        vbr: *vbr_raw,
        bitmap_runs,
        synth_bitmap,
        boot_code: Vec::new(),
    })
}

/// MFT-only open for compressed NTFS file-aware GHOs. Decompresses
/// just the first run (MFT), parses all MFT records, and returns a
/// partial index + lazy-scan state. The full run index is built
/// incrementally by `extend_ntfs_compressed_index` as reads arrive.
pub(super) fn open_ntfs_compressed_mft_only(
    reader: &mut SpanReader,
    data_start: u64,
    file_size: u64,
    compression: GhoCompression,
    vbr: &fs_pc::ntfs::NtfsVbr,
    vbr_raw: &[u8; 512],
) -> Result<(NtfsCompressedState, NtfsGhoIndex)> {
    use fs_pc::ntfs::{apply_fixup, parse_mft_attributes};

    let _ = compression;
    let (cluster_size, volume_size) = ntfs_vbr_geometry(vbr)?;
    let mft_record_size = vbr.mft_record_size as usize;

    let end = file_size;
    let io_chunk: usize = 4 * 1024 * 1024;
    let max_decomp_block: usize = FAST_LZ_BLOCK_SIZE + 1024;
    let mut decomp_buf = vec![0u8; max_decomp_block];
    let mut blocks = Vec::with_capacity(8_000);
    let mut decomp_offset: u64 = 0;
    // Decompressed bytes from `stream_base` on; consumed prefixes are dropped as the MFT is parsed.
    let mut stream: Vec<u8> = Vec::new();
    let mut stream_base: u64 = 0;
    let mut next_rec: u64 = 0;

    // Find first zlib header.
    let mut off = data_start;
    {
        let scan_len = ((end - off) as usize).min(8192);
        let mut scan_buf = vec![0u8; scan_len];
        reader.seek(SeekFrom::Start(off))?;
        let n = read_fully_or_eof(reader, &mut scan_buf)?;
        let mut found = false;
        for i in 0..n.saturating_sub(1) {
            if scan_buf[i] == 0x78 && scan_buf[i + 1] == 0x01 {
                off += i as u64;
                found = true;
                break;
            }
        }
        if !found {
            bail!("no zlib header found in NTFS compressed data region");
        }
    }

    // Decompress blocks until we have enough for the MFT.
    // Phase 1: find the first run header → get MFT cluster count.
    // Phase 2: accumulate MFT data → parse MFT records.
    let mut chunk_buf = vec![0u8; io_chunk + 64 * 1024];
    let mut chunk_start = off;
    reader.seek(SeekFrom::Start(chunk_start))?;
    let mut chunk_len = read_fully_or_eof(reader, &mut chunk_buf)?;
    let mut local = 0usize;

    let mut mft_data_start: Option<u64> = None;
    let mut mft_clusters: u64 = 0;
    let mut data_run_queue: Vec<(u64, u64)> = Vec::new();
    let mut bitmap_runs: Vec<(u64, u64)> = Vec::new();
    let mut mftmirr_run: Option<(u64, u64)> = None;
    let mut runs: Vec<NtfsGhoClusterRun> = Vec::new();
    let mut mft_parsed = false;

    let mut consecutive_fails = 0u32;
    while chunk_start + local as u64 + 4 < end && !mft_parsed {
        // Refill
        if local + 64 * 1024 > chunk_len && chunk_start + (chunk_len as u64) < end {
            let remaining = chunk_len - local;
            chunk_buf.copy_within(local..chunk_len, 0);
            chunk_start += local as u64;
            local = 0;
            reader.seek(SeekFrom::Start(chunk_start + remaining as u64))?;
            let extra = read_fully_or_eof(reader, &mut chunk_buf[remaining..])?;
            chunk_len = remaining + extra;
        }
        if local + 4 > chunk_len {
            break;
        }
        if chunk_buf[local] != 0x78 || chunk_buf[local + 1] != 0x01 {
            local += 1;
            consecutive_fails += 1;
            if consecutive_fails > 1_000_000 {
                // Ghost interleaves structural records between zlib blocks; a
                // long non-zlib gap must not abort the scan (doing so abandons
                // the rest of the stream and silently drops data). Reset and
                // keep scanning forward to resync at the next block. The high
                // cap only guards against u32 overflow.
                consecutive_fails = 0;
            }
            continue;
        }

        let avail = chunk_len - local;
        let input = &chunk_buf[local..local + avail];
        let mut decompress = flate2::Decompress::new(true);
        let mut total_in = 0usize;
        let mut ok = true;
        loop {
            let in_before = decompress.total_in() as usize;
            let out_before = decompress.total_out();
            // A Ghost block inflates to 32 KiB; more means this was not a block start.
            if out_before > GHO_MAX_INFLATE {
                ok = false;
                break;
            }
            let out_off = (decompress.total_out() as usize) % decomp_buf.len();
            match decompress.decompress(
                &input[total_in..],
                &mut decomp_buf[out_off..],
                flate2::FlushDecompress::None,
            ) {
                Ok(flate2::Status::Ok) => {
                    total_in = decompress.total_in() as usize;
                    if decompress.total_in() as usize == in_before
                        && decompress.total_out() == out_before
                    {
                        break;
                    }
                }
                Ok(flate2::Status::StreamEnd) => {
                    total_in = decompress.total_in() as usize;
                    break;
                }
                Ok(flate2::Status::BufError) | Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        let total_out = decompress.total_out() as usize;

        if !ok || total_out == 0 {
            local += 2;
            consecutive_fails += 1;
            if consecutive_fails > 1_000_000 {
                // Ghost interleaves structural records between zlib blocks; a
                // long non-zlib gap must not abort the scan (doing so abandons
                // the rest of the stream and silently drops data). Reset and
                // keep scanning forward to resync at the next block. The high
                // cap only guards against u32 overflow.
                consecutive_fails = 0;
            }
            continue;
        }

        blocks.push(NtfsCompressedBlock {
            file_offset: chunk_start + local as u64,
            comp_size: total_in as u32,
            decomp_offset,
            decomp_size: total_out as u32,
        });

        if total_out <= decomp_buf.len() {
            stream.extend_from_slice(&decomp_buf[..total_out]);
        } else {
            let mut big = vec![0u8; total_out];
            let mut d2 = flate2::Decompress::new(true);
            let _ = d2.decompress(
                &input[..total_in],
                &mut big,
                flate2::FlushDecompress::Finish,
            );
            stream.extend_from_slice(&big[..d2.total_out() as usize]);
        }
        decomp_offset += total_out as u64;
        local += total_in + 2;
        consecutive_fails = 0;

        // Try to find the run header and MFT in the accumulated stream.
        if mft_data_start.is_none() {
            let needle = &NTFS_RUN_HEADER_ENTRY1;
            if let Some(pos) = stream
                .windows(NTFS_RUN_HEADER_SIZE)
                .position(|w| w[..16] == *needle)
            {
                let hex: String = stream[pos..(pos + NTFS_RUN_HEADER_SIZE).min(stream.len())]
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<_>>()
                    .join(" ");
                log::info!(
                    "MFT header at stream pos {:#x}: {}",
                    stream_base + pos as u64,
                    hex
                );
                if let Some(cc) = parse_ntfs_run_header(&stream[pos..]) {
                    let mds = stream_base + pos as u64 + NTFS_RUN_HEADER_SIZE as u64;
                    mft_clusters = cc;
                    mft_data_start = Some(mds);
                    let mft_size = cc * cluster_size;
                    log::info!(
                        "Reading MFT: {} clusters ({:.1} MB) from decompressed offset {:#x}",
                        cc,
                        mft_size as f64 / (1024.0 * 1024.0),
                        mds
                    );
                    runs.push(NtfsGhoClusterRun {
                        lcn_start: vbr.mft_cluster,
                        cluster_count: cc,
                        file_offset: mds,
                        seq: 0,
                    });
                    // Nothing before the MFT's data is needed again.
                    let drop = (mds - stream_base) as usize;
                    stream.drain(..drop.min(stream.len()));
                    stream_base = mds;
                }
            } else {
                // Keep only a header's worth of tail so a split header is still found.
                let keep = NTFS_RUN_HEADER_SIZE - 1;
                if stream.len() > keep {
                    let drop = stream.len() - keep;
                    stream.drain(..drop);
                    stream_base += drop as u64;
                }
            }
        }

        if let Some(mds) = mft_data_start {
            let mft_size = mft_clusters * cluster_size;
            let num_records = mft_size / mft_record_size as u64;
            // Parse whole records as they arrive, then drop them, so the MFT is never held whole.
            while next_rec < num_records {
                let rec_abs = mds + next_rec * mft_record_size as u64;
                let rec_off = (rec_abs - stream_base) as usize;
                if rec_off + mft_record_size > stream.len() {
                    break;
                }
                next_rec += 1;
                let rec = &stream[rec_off..rec_off + mft_record_size];
                if &rec[0..4] != b"FILE" {
                    continue;
                }
                let rec_num = u32::from_le_bytes([rec[44], rec[45], rec[46], rec[47]]);
                if rec_num == 8 {
                    continue;
                }
                let mut rec_buf = rec.to_vec();
                let _ = apply_fixup(&mut rec_buf);
                let attrs = parse_mft_attributes(&rec_buf, vbr.mft_record_size);
                for attr in &attrs {
                    if attr.resident || attr.data_runs.is_empty() {
                        continue;
                    }
                    // Record 0 is $MFT itself. Its $DATA runs describe
                    // every MFT fragment; fragment 0 is already registered
                    // as the MFT run above, so skip it here, but queue the
                    // later fragments so the lazy scan maps them. Without
                    // this, high-numbered MFT records (which live in those
                    // fragments) read back as zeros -> "invalid magic".
                    let skip_first = rec_num == 0 && attr.attr_type == 0x80;
                    for (i, dr) in attr.data_runs.iter().enumerate() {
                        if skip_first && i == 0 {
                            continue;
                        }
                        // Skip sparse runs (cluster_offset == 0 = hole);
                        // queuing them as lcn=0 causes $Boot cross-links.
                        if dr.cluster_offset > 0 && dr.length > 0 {
                            // $MFTMirr ($DATA, rec 1): synthesized from the
                            // $MFT data, not from a stream run (Ghost omits
                            // the mirror content). Capture, don't queue. See
                            // the uncompressed path for the full rationale.
                            if rec_num == 1 && attr.attr_type == 0x80 && mftmirr_run.is_none() {
                                mftmirr_run = Some((dr.cluster_offset as u64, dr.length));
                                continue;
                            }
                            data_run_queue.push((dr.cluster_offset as u64, dr.length));
                            if rec_num == 6 && attr.attr_type == 0x80 {
                                bitmap_runs.push((dr.cluster_offset as u64, dr.length));
                            }
                        }
                    }
                }
            }
            let parsed_to = mds + next_rec * mft_record_size as u64;
            let drop = (parsed_to - stream_base) as usize;
            stream.drain(..drop.min(stream.len()));
            stream_base = parsed_to;
            if next_rec == num_records && stream_base + stream.len() as u64 >= mds + mft_size {
                // Synthesize $MFTMirr: map its clusters back to the start of
                // $MFT's stream data (the mirror is a verbatim copy of $MFT's
                // first records). Without this the mirror reads as zeros and
                // chkdsk flags "$MFTMirr ($DATA) corrupt / cross-linked".
                if let Some((mirr_lcn, mirr_cc)) = mftmirr_run {
                    let cc = mirr_cc.min(mft_clusters);
                    if cc > 0 {
                        runs.push(NtfsGhoClusterRun {
                            lcn_start: mirr_lcn,
                            cluster_count: cc,
                            file_offset: mds,
                            seq: 0,
                        });
                        log::info!(
                            "Synthesized $MFTMirr: {} cluster(s) at lcn {} mapped to $MFT data",
                            cc,
                            mirr_lcn
                        );
                    }
                }
                log::info!(
                    "MFT parsed: {} data-run entries from {} MFT records",
                    data_run_queue.len(),
                    num_records
                );
                mft_parsed = true;
            }
        }
    }

    if !mft_parsed {
        bail!("failed to read MFT from compressed NTFS stream");
    }

    let mds = mft_data_start.unwrap();
    let mft_end = mds + mft_clusters * cluster_size;
    // The resume position: where we stopped in the compressed file.
    let resume_file_offset = chunk_start + local as u64;

    // Keep the stream tail from mft_end onward for gap parsing when
    // the lazy scan resumes.
    let stream_tail_start = mft_end.saturating_sub(stream_base) as usize;
    let leftover_stream = if stream_tail_start < stream.len() {
        stream[stream_tail_start..].to_vec()
    } else {
        Vec::new()
    };

    runs.sort_by_key(|r| r.lcn_start);

    log::info!(
        "MFT-only open: {} blocks, {} run(s), resume scan at {:#x}",
        blocks.len(),
        runs.len(),
        resume_file_offset,
    );

    // $Bitmap is synthesized lazily on first read (see read_ntfs_file_aware_into):
    // the complete run set isn't known until the lazy scan finishes, and most
    // files' MFT records are stored inline in the stream, not in data_run_queue.
    // Leaving synth_bitmap empty signals "not built yet".

    let comp_state = NtfsCompressedState {
        blocks,
        compression,
        total_decompressed: decomp_offset,
        cache_block: None,
        cache_buf: Vec::new(),
        lazy_scan: Some(NtfsLazyScanState {
            file_offset: resume_file_offset,
            decomp_offset,
            file_size,
            vbr: vbr.clone(),
            cluster_size,
            pending_lcns: Vec::new(),
            pending_group_start: 0,
            data_run_queue,
            prev_data_end: mft_end,
            stream: leftover_stream,
            stream_base: mft_end,
            skip_data_bytes: 0,
            search_resume: mft_end,
            assigned_lcns: runs.iter().map(|r| r.lcn_start).collect(),
        }),
        cache_key: None,
    };
    let index = NtfsGhoIndex {
        runs,
        volume_size,
        cluster_size,
        vbr: *vbr_raw,
        bitmap_runs,
        synth_bitmap: Vec::new(), // built lazily on first $Bitmap read
        boot_code: Vec::new(),    // recovered by prepare_full_image (export)
    };
    Ok((comp_state, index))
}

/// Pop the FIRST `(lcn, cc)` entry whose cluster count matches `cc` and whose
/// LCN isn't already assigned to another run. Ghost emits cluster runs in
/// MFT-walk order — record order, then attribute order within a record, then
/// fragment order within an attribute — and the MFT data-run queue is built in
/// that same order, so the correct match for a stream run is the EARLIEST
/// unassigned entry of the right size. The assigned-LCN check prevents two
/// stream runs from landing on the same clusters (Ghost re-declares some runs
/// both inline and in the MFT queue). Returns the chosen LCN, or `None`.
///
/// Used for the global MFT data-run queue (the Phase-3 fallback and the
/// compressed lazy path's secondary lookup), where the whole list is one
/// MFT-ordered group. For the inline `pending_lcns` list, which interleaves a
/// freshly parsed gap's entries with stale orphans, use [`match_inline_run`].
pub(super) fn pop_unassigned_lcn(
    list: &mut Vec<(u64, u64)>,
    cc: u64,
    assigned: &std::collections::HashSet<u64>,
) -> Option<u64> {
    let idx = list
        .iter()
        .position(|&(lcn, len)| len == cc && !assigned.contains(&lcn))?;
    Some(list.remove(idx).0)
}

/// Match a stream run of size `cc` to an inline-MFT `(lcn, cc)` entry.
///
/// `pending_lcns` accumulates the data runs parsed from inline FILE records as
/// gaps are scanned. Ghost writes each gap's records immediately before the
/// cluster data they describe, and emits that data in MFT-walk order, so the
/// correct entry for a stream run is the EARLIEST unassigned entry of size `cc`
/// within the most recently parsed gap — the suffix `list[group_start..]`.
/// Matching from the back instead (`rposition`) reverses every same-`cc` group:
/// e.g. `$Secure`'s `$SDH`/`$SII` index-allocation blocks get swapped and a
/// fragmented file's `$DATA` extents get reversed (verified against
/// `smallNTFS.GHO`, where it produced chkdsk `$SDH`/`$SII`/`$I30` index errors).
/// Matching from the front of the whole list instead grabs stale orphans: Ghost
/// re-declares some already-emitted runs in a later gap, leaving entries that
/// never get a stream run (e.g. a leftover `(1310297, 1)` ahead of the real
/// `$SDH`/`$SII` pair). So: search the freshest group forward first, and only
/// fall back to older orphaned entries (also forward) if it has no match.
///
/// On success removes the chosen entry, fixes up `*group_start`, and returns the
/// LCN. The assigned-LCN check prevents double-mapping onto another file's
/// clusters.
pub(super) fn match_inline_run(
    list: &mut Vec<(u64, u64)>,
    group_start: &mut usize,
    cc: u64,
    assigned: &std::collections::HashSet<u64>,
) -> Option<u64> {
    let gs = (*group_start).min(list.len());
    let idx = list[gs..]
        .iter()
        .position(|&(lcn, len)| len == cc && !assigned.contains(&lcn))
        .map(|i| gs + i)
        .or_else(|| {
            list[..gs]
                .iter()
                .position(|&(lcn, len)| len == cc && !assigned.contains(&lcn))
        })?;
    if idx < *group_start {
        *group_start -= 1;
    }
    Some(list.remove(idx).0)
}

/// Extend the NTFS compressed run index by scanning forward in the
/// compressed stream until `target_lcn` is found or the file is exhausted.
/// Called lazily from `read_ntfs_file_aware_into` when a read hits an
/// unmapped LCN.
pub(super) fn extend_ntfs_compressed_index(
    inner: &mut SpanReader,
    index: &mut NtfsGhoIndex,
    comp: &mut NtfsCompressedState,
    target_lcn: u64,
) -> std::io::Result<bool> {
    let Some(scan) = &mut comp.lazy_scan else {
        return Ok(false);
    };

    let io_chunk: usize = 4 * 1024 * 1024;
    let max_decomp_block: usize = FAST_LZ_BLOCK_SIZE + 1024;
    let mut decomp_buf = vec![0u8; max_decomp_block];
    let mut discard = vec![0u8; 256 * 1024];
    let end = scan.file_size;

    let mut chunk_buf = vec![0u8; io_chunk + 64 * 1024];
    let mut chunk_start = scan.file_offset;
    // A read error is the image failing, not padding: report it rather than serve zeros.
    inner.seek(SeekFrom::Start(chunk_start))?;
    let mut chunk_len = read_fully_or_eof(inner, &mut chunk_buf).map_err(disk_core::io_other)?;
    let mut local = 0usize;
    let mut found_target = false;
    let mut consecutive_fails = 0u32;
    let mut decompress = flate2::Decompress::new(true);
    let scan_start_offset = chunk_start;
    let scan_start_time = std::time::Instant::now();
    let mut last_progress_log = scan_start_time;
    log::info!(
        "Lazy scan starting: target_lcn={}, from offset {:#x}/{:#x} ({:.1}% of file)",
        target_lcn,
        scan_start_offset,
        end,
        (scan_start_offset as f64 / end as f64) * 100.0
    );

    // Note: we intentionally scan to EOF rather than stopping at the target
    // LCN. Stopping early would leave the target run's cluster-data blocks
    // un-decompressed (and thus absent from comp.blocks), so the read that
    // triggered the scan would get zero bytes back. A full pass records every
    // block once; the index + block table then persist for all later reads.
    while chunk_start + local as u64 + 4 < end {
        // Periodic progress report
        if last_progress_log.elapsed().as_secs() >= 2 {
            let cur = chunk_start + local as u64;
            let scanned = cur.saturating_sub(scan_start_offset);
            let elapsed = scan_start_time.elapsed().as_secs_f64();
            let mb_per_s = (scanned as f64 / 1_048_576.0) / elapsed.max(0.001);
            log::info!(
                "Lazy scan progress: at {:#x}/{:#x} ({:.1}%), {} runs, {} blocks, {:.1} MB/s",
                cur,
                end,
                (cur as f64 / end as f64) * 100.0,
                index.runs.len(),
                comp.blocks.len(),
                mb_per_s
            );
            last_progress_log = std::time::Instant::now();
        }

        // Refill
        if local + 64 * 1024 > chunk_len && chunk_start + (chunk_len as u64) < end {
            let remaining = chunk_len - local;
            chunk_buf.copy_within(local..chunk_len, 0);
            chunk_start += local as u64;
            local = 0;
            inner.seek(SeekFrom::Start(chunk_start + remaining as u64))?;
            let extra = read_fully_or_eof(inner, &mut chunk_buf[remaining..])
                .map_err(disk_core::io_other)?;
            chunk_len = remaining + extra;
        }
        if local + 4 > chunk_len {
            break;
        }
        if chunk_buf[local] != 0x78 || chunk_buf[local + 1] != 0x01 {
            local += 1;
            consecutive_fails += 1;
            if consecutive_fails > 1_000_000 {
                // Ghost interleaves structural records between zlib blocks; a
                // long non-zlib gap must not abort the scan (doing so abandons
                // the rest of the stream and silently drops data). Reset and
                // keep scanning forward to resync at the next block. The high
                // cap only guards against u32 overflow.
                consecutive_fails = 0;
            }
            continue;
        }

        let avail = chunk_len - local;
        let input = &chunk_buf[local..local + avail];

        // If skipping cluster data, use discard buffer.
        if scan.skip_data_bytes > 0 {
            decompress.reset(true);
            let mut total_in = 0usize;
            let mut ok = true;
            loop {
                let in_before = decompress.total_in() as usize;
                let out_before = decompress.total_out();
                // A Ghost block inflates to 32 KiB; more means this was not a block start.
                if out_before > GHO_MAX_INFLATE {
                    ok = false;
                    break;
                }
                match decompress.decompress(
                    &input[total_in..],
                    &mut discard[..],
                    flate2::FlushDecompress::None,
                ) {
                    Ok(flate2::Status::Ok) => {
                        total_in = decompress.total_in() as usize;
                        if decompress.total_in() as usize == in_before
                            && decompress.total_out() == out_before
                        {
                            break;
                        }
                    }
                    Ok(flate2::Status::StreamEnd) => {
                        total_in = decompress.total_in() as usize;
                        break;
                    }
                    Ok(flate2::Status::BufError) | Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
            let total_out = decompress.total_out() as usize;
            if !ok || total_out == 0 {
                local += 2;
                consecutive_fails += 1;
                if consecutive_fails > 1_000_000 {
                    consecutive_fails = 0;
                }
                continue;
            }
            comp.blocks.push(NtfsCompressedBlock {
                file_offset: chunk_start + local as u64,
                comp_size: total_in as u32,
                decomp_offset: scan.decomp_offset,
                decomp_size: total_out as u32,
            });
            scan.decomp_offset += total_out as u64;
            comp.total_decompressed = scan.decomp_offset;
            // If this block straddles the end of the run's cluster data, keep
            // the metadata tail (decompressed into `discard`) so the gap's
            // inline MFT records aren't lost. stream_base was set to
            // run_data_end when the skip was triggered, so the tail's bytes
            // begin exactly at the (now empty) stream's base.
            if (total_out as u64) > scan.skip_data_bytes {
                let tail_start = scan.skip_data_bytes as usize;
                if tail_start < total_out && total_out <= discard.len() {
                    scan.stream
                        .extend_from_slice(&discard[tail_start..total_out]);
                }
                scan.skip_data_bytes = 0;
            } else {
                scan.skip_data_bytes -= total_out as u64;
            }
            local += total_in + 2;
            consecutive_fails = 0;
            continue;
        }

        // Metadata block — decompress fully.
        decompress.reset(true);
        let mut total_in = 0usize;
        let mut ok = true;
        loop {
            let in_before = decompress.total_in() as usize;
            let out_before = decompress.total_out();
            // A Ghost block inflates to 32 KiB; more means this was not a block start.
            if out_before > GHO_MAX_INFLATE {
                ok = false;
                break;
            }
            let out_off = (decompress.total_out() as usize) % decomp_buf.len();
            match decompress.decompress(
                &input[total_in..],
                &mut decomp_buf[out_off..],
                flate2::FlushDecompress::None,
            ) {
                Ok(flate2::Status::Ok) => {
                    total_in = decompress.total_in() as usize;
                    if decompress.total_in() as usize == in_before
                        && decompress.total_out() == out_before
                    {
                        break;
                    }
                }
                Ok(flate2::Status::StreamEnd) => {
                    total_in = decompress.total_in() as usize;
                    break;
                }
                Ok(flate2::Status::BufError) | Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        let total_out = decompress.total_out() as usize;
        if !ok || total_out == 0 {
            local += 2;
            consecutive_fails += 1;
            if consecutive_fails > 1_000_000 {
                // Ghost interleaves structural records between zlib blocks; a
                // long non-zlib gap must not abort the scan (doing so abandons
                // the rest of the stream and silently drops data). Reset and
                // keep scanning forward to resync at the next block. The high
                // cap only guards against u32 overflow.
                consecutive_fails = 0;
            }
            continue;
        }

        comp.blocks.push(NtfsCompressedBlock {
            file_offset: chunk_start + local as u64,
            comp_size: total_in as u32,
            decomp_offset: scan.decomp_offset,
            decomp_size: total_out as u32,
        });

        if total_out <= decomp_buf.len() {
            scan.stream.extend_from_slice(&decomp_buf[..total_out]);
        }
        scan.decomp_offset += total_out as u64;
        comp.total_decompressed = scan.decomp_offset;
        local += total_in + 2;
        consecutive_fails = 0;

        // Search for run headers in accumulated stream.
        let stream_abs_len = scan.stream_base + scan.stream.len() as u64;

        // Tracks whether the inner search loop reached EOH (end of haystack)
        // without finding a match. Only then is it safe to advance
        // `search_resume` past the current stream — otherwise we risk
        // skipping a needle that was found but couldn't be parsed because
        // entry C extended past the current stream.
        let mut full_scan_completed = false;
        loop {
            let search_start = scan.prev_data_end.max(scan.search_resume);
            let Some(search_local) =
                stream_local_offset(search_start, scan.stream_base, scan.stream.len())
            else {
                break;
            };
            if search_local + NTFS_RUN_B_HEADER_SIZE > scan.stream.len() {
                // Not enough bytes from search_start onward to even contain
                // a complete header — wait for the next block.
                break;
            }
            // Subsequent (non-MFT) runs use the 26-byte entry B + entry C
            // header. Byte 7 of entry B is a run sequence number, so we
            // match with a wildcard there.
            let haystack = &scan.stream[search_local..];
            let mut idx_opt: Option<usize> = None;
            for i in 0..haystack.len().saturating_sub(16) + 1 {
                if match_run_entry_b(&haystack[i..i + 16]) {
                    idx_opt = Some(i);
                    break;
                }
            }
            let Some(idx) = idx_opt else {
                // Scanned the entire haystack — no needle. Safe to advance
                // search_resume past this stream extent.
                full_scan_completed = true;
                break;
            };
            let hdr_local = search_local + idx;
            let hdr_abs = scan.stream_base + hdr_local as u64;
            if hdr_local + NTFS_RUN_B_HEADER_SIZE > scan.stream.len() {
                // Found a needle but entry C extends past current stream.
                // Resume right here next pass so we re-find and parse it.
                if scan.search_resume < hdr_abs {
                    scan.search_resume = hdr_abs;
                }
                break;
            }
            let Some((seq, cc)) = parse_ntfs_run_b_header(&scan.stream[hdr_local..]) else {
                scan.prev_data_end = hdr_abs + 16;
                continue;
            };
            if cc == 0 {
                scan.prev_data_end = hdr_abs + NTFS_RUN_B_HEADER_SIZE as u64;
                continue;
            }

            let run_data_start = hdr_abs + NTFS_RUN_B_HEADER_SIZE as u64;
            let run_data_end = run_data_start + cc * scan.cluster_size;

            // Parse inline MFT records in the gap since the last run. Each
            // gap holds the FILE records whose data runs describe the runs
            // that follow, so this populates pending_lcns just-in-time.
            if let Some(gap_local_start) =
                stream_local_offset(scan.prev_data_end, scan.stream_base, scan.stream.len())
            {
                if hdr_local > gap_local_start {
                    let gap = scan.stream[gap_local_start..hdr_local].to_vec();
                    let len_before = scan.pending_lcns.len();
                    parse_inline_mft_from_buf(&gap, &scan.vbr, &mut scan.pending_lcns);
                    // A non-empty gap starts a fresh group (see match_inline_run).
                    if scan.pending_lcns.len() > len_before {
                        scan.pending_group_start = len_before;
                    }
                }
            }

            // Match LCN in MFT-walk order: prefer the freshest inline group
            // (pending_lcns; see match_inline_run), then fall back to the global
            // MFT data-run queue. Skip candidates whose LCN is already assigned —
            // Ghost re-declares some files' runs both inline and in the queue, so
            // a naive match double-assigns and cross-links.
            let lcn = match_inline_run(
                &mut scan.pending_lcns,
                &mut scan.pending_group_start,
                cc,
                &scan.assigned_lcns,
            )
            .or_else(|| pop_unassigned_lcn(&mut scan.data_run_queue, cc, &scan.assigned_lcns));

            if let Some(lcn) = lcn {
                scan.assigned_lcns.insert(lcn);
                index.runs.push(NtfsGhoClusterRun {
                    lcn_start: lcn,
                    cluster_count: cc,
                    file_offset: run_data_start,
                    seq,
                });
                if target_lcn >= lcn && target_lcn < lcn + cc {
                    found_target = true;
                }
            } else {
                // Even when no LCN matches, record the stream run so the
                // fixup can reassign by seq later. lcn_start = u64::MAX is a
                // sentinel meaning "no original lcn known".
                index.runs.push(NtfsGhoClusterRun {
                    lcn_start: u64::MAX,
                    cluster_count: cc,
                    file_offset: run_data_start,
                    seq,
                });
            }

            scan.prev_data_end = run_data_end;
            if run_data_end > stream_abs_len {
                scan.skip_data_bytes = run_data_end - stream_abs_len;
                // The remaining run data isn't in the stream yet; the skip
                // path will decompress+discard it. Reset the rolling buffer
                // so it stays contiguous: after the skip completes, new
                // metadata bytes begin at run_data_end.
                scan.stream.clear();
                scan.stream_base = run_data_end;
                scan.search_resume = run_data_end;
                break;
            }
        }

        // Only advance search_resume if the inner loop scanned the entire
        // haystack without finding a needle. If it broke for "insufficient
        // bytes for entry C", search_resume was already pinned to that
        // needle position so we re-find it next pass.
        if full_scan_completed {
            let stream_end_abs = scan.stream_base + scan.stream.len() as u64;
            let new_resume = stream_end_abs.saturating_sub(15);
            if new_resume > scan.search_resume {
                scan.search_resume = new_resume;
            }
        }

        // Drain processed stream data.
        if let Some(drain_local) =
            stream_local_offset(scan.prev_data_end, scan.stream_base, scan.stream.len())
        {
            let drain_local = drain_local.min(scan.stream.len());
            if drain_local > 4 * 1024 * 1024 {
                let keep = scan.stream.len() - drain_local;
                scan.stream.copy_within(drain_local.., 0);
                scan.stream.truncate(keep);
                scan.stream_base += drain_local as u64;
            }
        }
    }

    // Save resume position.
    scan.file_offset = chunk_start + local as u64;
    let final_file_offset = scan.file_offset;
    let reached_eof = final_file_offset + 4 >= end;

    // Re-sort runs after adding new ones.
    index.runs.sort_by_key(|r| r.lcn_start);

    let total_decompressed = comp.total_decompressed;
    let run_count = index.runs.len();
    let block_count = comp.blocks.len();
    // If we reached EOF, mark scan as complete.
    if reached_eof {
        comp.lazy_scan = None;
        log::info!(
            "Lazy scan complete (EOF): {run_count} runs, {block_count} blocks, \
             file_offset={final_file_offset} end={end} total_decompressed={total_decompressed}",
        );
        // The index + block table are now complete; cache them so a later
        // re-open of this archive skips the rescan entirely.
        if let Some(key) = comp.cache_key.clone() {
            ntfs_scan_cache_store(&key, index, comp);
        }
    } else {
        log::warn!(
            "Lazy scan STOPPED EARLY at file_offset={final_file_offset} of end={end} \
             ({run_count} runs, {block_count} blocks, total_decompressed={total_decompressed}) \
             — remaining compressed data was not decoded",
        );
    }

    Ok(found_target)
}

/// Try to open an NTFS file-aware GHO. Returns the reader mode and
/// volume size on success.
pub(super) fn try_open_ntfs_file_aware(
    inner: &mut SpanReader,
    file_size: u64,
    compression: GhoCompression,
    cache_key: Option<NtfsScanKey>,
) -> Result<(GhoReaderMode, u64)> {
    use fs_pc::ntfs::parse_vbr;

    // Find the NTFS VBR in the GHPR metadata region.
    let (vbr_off, vbr_raw) = find_ntfs_vbr_in_header(inner, 0x200, file_size.min(0x4000))
        .context("locating NTFS VBR in GHPR metadata")?;
    log::info!("NTFS VBR found at file offset {:#x}", vbr_off);

    let vbr = parse_vbr(&vbr_raw).map_err(|e| anyhow!("parsing NTFS VBR: {e}"))?;
    log::info!(
        "NTFS volume: {} bytes/sector, {} sectors/cluster, {} total sectors, MFT at LCN {}",
        vbr.bytes_per_sector,
        vbr.sectors_per_cluster,
        vbr.total_sectors,
        vbr.mft_cluster
    );

    if matches!(compression, GhoCompression::None) {
        let index = index_ntfs_file_aware(inner, file_size, &vbr, &vbr_raw)
            .context("building NTFS cluster run index")?;
        let served_size = ntfs_served_size(&index);
        return Ok((
            GhoReaderMode::NtfsFileAware {
                index,
                last_run_hint: 0,
                compressed: None,
            },
            served_size,
        ));
    }

    // Compressed NTFS file-aware. A completed lazy scan for this exact archive
    // may already be cached from a prior open (the GUI re-opens a fresh reader
    // per browse/extract operation) — reuse it and skip both the MFT
    // decompression and the whole-stream forward scan.
    if let Some(key) = &cache_key {
        if let Some(snap) = ntfs_scan_cache_get(key) {
            log::info!(
                "Reusing cached NTFS lazy-scan ({} runs, {} blocks) — skipping rescan",
                snap.runs.len(),
                snap.blocks.len()
            );
            return Ok(ntfs_mode_from_cache(&snap, key.clone()));
        }
    }

    // Compressed NTFS file-aware: MFT-only open. Decompress just the
    // first run (MFT) and parse it. The rest of the index is built
    // lazily on demand when reads hit unmapped LCNs.
    log::info!("Opening compressed NTFS file-aware (MFT-only, lazy index)...");
    let data_start = vbr_off + 512;
    let (mut comp_state, index) =
        open_ntfs_compressed_mft_only(inner, data_start, file_size, compression, &vbr, &vbr_raw)
            .context("MFT-only open of compressed NTFS file-aware")?;
    // Remember the cache key so the completed scan gets stored for reuse.
    comp_state.cache_key = cache_key;
    let served_size = ntfs_served_size(&index);
    Ok((
        GhoReaderMode::NtfsFileAware {
            index,
            last_run_hint: 0,
            compressed: Some(comp_state),
        },
        served_size,
    ))
}

/// What first-4-byte magic the cluster content for an NTFS attribute is expected
/// to start with. Used by [`fixup_ntfs_typed_misalignments`] to detect cluster
/// runs the matcher placed at the wrong `lcn_start` when multiple MFT attributes
/// share the same `cluster_count` (the dominant ambiguity is `cc=2`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum ExpectedMagic {
    /// `$INDEX_ALLOCATION` (attr type `0xa0`) blocks start with `INDX`.
    Indx,
    /// `$MFT` (rec 0) `$DATA` fragments are sequences of `FILE` records.
    File,
    /// `$LogFile` (rec 2) `$DATA` blocks start with `RSTR` (restart) or `RCRD`
    /// (log record).
    Logfile,
    /// Pre-Windows-2000 per-file `$SECURITY_DESCRIPTOR` (attr type `0x50`) and
    /// the entries in `$Secure $DATA "$SDS"`. The first byte is the SD revision
    /// (always 1), byte 1 is sbz1 (always 0), byte 3's high bit is
    /// `SE_SELF_RELATIVE` (almost always set for stored SDs). That's a 4-byte
    /// fingerprint stronger than a magic number even though it's not literal.
    SecurityDescriptor,
}

impl ExpectedMagic {
    /// Predict the magic for a non-resident attribute, given the MFT record that
    /// **owns** it (i.e. the base record number, chasing `$ATTRIBUTE_LIST` for
    /// extension records). `None` for attributes whose content is opaque (user
    /// `$DATA`, `$Secure $DATA $SDS`, etc.) — no useful prediction.
    pub(super) fn from_attr(owning_rec: u64, attr_type: u32) -> Option<Self> {
        match attr_type {
            0xa0 => Some(Self::Indx),
            // $MFT (rec 0) and $MFTMirr (rec 1) both store FILE records.
            0x80 if owning_rec == 0 || owning_rec == 1 => Some(Self::File),
            0x80 if owning_rec == 2 => Some(Self::Logfile),
            // Per-file embedded security descriptor (old-style NTFS).
            0x50 => Some(Self::SecurityDescriptor),
            _ => None,
        }
    }

    /// Classify an observed magic against the predictable types.
    pub(super) fn from_observed(magic: &[u8; 4]) -> Option<Self> {
        if magic == b"INDX" {
            Some(Self::Indx)
        } else if magic == b"FILE" {
            Some(Self::File)
        } else if magic == b"RSTR" || magic == b"RCRD" {
            Some(Self::Logfile)
        } else if magic[0] == 0x01 && magic[1] == 0x00 && (magic[3] & 0x80) != 0 {
            // SD revision 1 + sbz1=0 + SE_SELF_RELATIVE control bit. Byte 2
            // (control low) varies; byte 3 (control high) is 0x80 or
            // 0x80|other-bits.
            Some(Self::SecurityDescriptor)
        } else {
            None
        }
    }
}

/// Read up to `out.len()` bytes directly from `file_offset` — bypassing the
/// `lcn → run` index lookup so an overlapping-runs ambiguity doesn't corrupt
/// the result. Handles both uncompressed (read straight from the `SpanReader`)
/// and zlib-compressed GHOs (route through `NtfsDecompressingReader`). Returns
/// the number of bytes actually read; short reads (or 0) signal EOF / missing
/// decompressed coverage for that offset.
pub(super) fn peek_run_bytes(
    inner: &mut SpanReader,
    compressed: Option<&mut NtfsCompressedState>,
    file_offset: u64,
    out: &mut [u8],
) -> usize {
    if let Some(cs) = compressed {
        if file_offset >= cs.total_decompressed {
            return 0;
        }
        let mut rdr = NtfsDecompressingReader {
            inner,
            state: cs,
            position: file_offset,
        };
        let mut got = 0;
        while got < out.len() {
            match rdr.read(&mut out[got..]) {
                Ok(0) => break,
                Ok(n) => got += n,
                Err(_) => break,
            }
        }
        return got;
    }
    if inner.seek(SeekFrom::Start(file_offset)).is_err() {
        return 0;
    }
    let mut got = 0;
    while got < out.len() {
        match inner.read(&mut out[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(_) => break,
        }
    }
    got
}

/// Per-run expected content, derived from the MFT.
#[derive(Debug, Clone, Copy)]
pub(super) struct TypedExpect {
    /// Run length in clusters.
    pub(super) cc: u64,
    /// Predicted content magic, or `None` for opaque attribute kinds.
    pub(super) magic: Option<ExpectedMagic>,
    /// For `Indx`, the expected `self_vcn` of the FIRST INDX block in this run.
    /// (Each subsequent INDX block self_vcn is `+= idx_record_size / cluster_size`,
    /// which is `1` on all volumes we've seen since idx_record_size == cluster_size.)
    pub(super) expected_vcn: u64,
}

/// Post-scan content-aware index rebuild.
///
/// The streaming matcher in [`index_ntfs_file_aware`] / [`extend_ntfs_compressed_index`]
/// disambiguates same-`cc` MFT entries by MFT-record order alone. When Ghost's
/// stored stream order doesn't match MFT-record order — the dominant ambiguity
/// is `cc=2` (the default initial `$INDEX_ALLOCATION` allocation) — runs land
/// at the wrong `lcn_start` and the reader serves other files' bytes at those
/// LCNs. chkdsk then sees garbage `INDX` blocks, flags directories as corrupt,
/// and (with `/f`) orphans thousands of files.
///
/// **Strategy.** Instead of trying to patch the matcher's mistakes via swaps
/// (which are hard to get right when same-`cc` entries are pervasive and the
/// matcher's index has *overlapping* LCN ranges), this pass treats the MFT as
/// ground truth and rebuilds `index.runs` from scratch:
///
/// 1. **MFT scan.** Walk every FILE record; for each non-resident attribute,
///    record `(lcn, cc, magic, expected_vcn)`. Magic is INDX for
///    `$INDEX_ALLOCATION`, FILE for `$MFT $DATA`, RSTR|RCRD for `$LogFile
///    $DATA`, `None` for opaque attrs (user `$DATA`, `$Secure $DATA $SDS`,
///    etc.). The base-record id (FILE header offset 0x20) resolves
///    `$ATTRIBUTE_LIST` extension records back to the owning base — that's how
///    `$Secure`'s `$SDH` index runs (which live in an extension record, not
///    rec 9 itself) get the correct INDX expectation.
/// 2. **Observe.** For every run in the existing `index.runs`, peek the first
///    24 bytes directly from its `file_offset` (bypassing the binary_search-on-
///    LCN lookup, which is what overlapping runs corrupt). Record kind + INDX
///    `self_vcn`. The (file_offset, kind, self_vcn, cluster_count) tuple is
///    what we have to *place*.
/// 3. **Assign.** For each MFT entry of cc=K, find the observed run with cc=K
///    whose content matches the entry's predicted magic + (for INDX) vcn. That
///    run's `file_offset` becomes the new entry's `file_offset` at `lcn = entry.lcn`.
///    For entries with no magic prediction (user data), match by cc alone in
///    MFT order — same heuristic as the original matcher but applied at the
///    end, after all magic-typed assignments have claimed their slots.
/// 4. **Replace.** Drop the old `index.runs` entirely; substitute the rebuilt
///    list. Re-sort by `lcn_start`. The new list has *no overlaps* because
///    every entry came from a distinct MFT data-run declaration, and NTFS
///    guarantees non-overlapping cluster claims within a well-formed volume.
///
/// The MFT run itself (rec 0 fragment 0 at `vbr.mft_cluster`) is preserved
/// verbatim — it was set explicitly at scan start, never via the ambiguous cc
/// queue, so it's already correct.
///
/// Conservative for opaque attrs: when MFT order disagrees with stream order,
/// the user-`$DATA` portion may still be cross-linked. But every metadata
/// stream (`$MFT`, `$LogFile`, every `$INDEX_ALLOCATION`) is placed by content,
/// which is what chkdsk reads.
pub(super) fn fixup_ntfs_typed_misalignments(
    inner: &mut SpanReader,
    index: &mut NtfsGhoIndex,
    compressed: &mut Option<NtfsCompressedState>,
    vbr: &fs_pc::ntfs::NtfsVbr,
) {
    use fs_pc::ntfs::{apply_fixup, parse_mft_attributes};

    let cluster_size = index.cluster_size;
    let mft_record_size = vbr.mft_record_size as usize;

    let Some(mft_run) = index
        .runs
        .iter()
        .find(|r| r.lcn_start == vbr.mft_cluster)
        .cloned()
    else {
        log::warn!("NTFS fixup: MFT run not found in index, skipping");
        return;
    };

    // The MFT's first fragment is only part of the MFT — read $MFT (rec 0) to
    // discover the total size via its $DATA $DATA attribute. Without this we'd
    // miss every MFT record beyond the first fragment, which on this corpus is
    // ~3 % of the table.
    let mut rec0 = vec![0u8; mft_record_size];
    {
        let mft_pos = vbr.mft_cluster * cluster_size;
        let mut total = 0;
        let mut hint = 0usize;
        while total < mft_record_size {
            let n = match read_ntfs_file_aware_into(
                inner,
                index,
                &mut hint,
                compressed,
                mft_pos + total as u64,
                &mut rec0[total..],
            ) {
                Ok(n) => n,
                Err(_) => break,
            };
            if n == 0 {
                break;
            }
            total += n;
        }
    }
    let _ = apply_fixup(&mut rec0);
    // $MFT's own $DATA runs, as (vcn_start, cluster_count, lcn_start) in order.
    // Used to place FILE-magic MFT-fragment runs deterministically by content:
    // an MFT fragment's first record carries its own record number, which fixes
    // the fragment's VCN, and these runs map that VCN to its LCN.
    let mut mft_data_runs: Vec<(u64, u64, u64)> = Vec::new();
    let mft_total_bytes = {
        let attrs = parse_mft_attributes(&rec0, vbr.mft_record_size);
        let mut total = 0u64;
        for a in &attrs {
            if !a.resident && a.attr_type == 0x80 {
                let mut vcn = a.starting_vcn;
                for dr in &a.data_runs {
                    if dr.cluster_offset > 0 && dr.length > 0 {
                        mft_data_runs.push((vcn, dr.length, dr.cluster_offset as u64));
                    }
                    vcn += dr.length;
                    total += dr.length * cluster_size;
                }
            }
        }
        if total == 0 {
            mft_run.cluster_count * cluster_size
        } else {
            total
        }
    };
    // Map an absolute $MFT VCN to its on-disk LCN via the $MFT $DATA runs.
    let mft_vcn_to_lcn = |vcn: u64| -> Option<u64> {
        for &(vstart, len, lcn) in &mft_data_runs {
            if vcn >= vstart && vcn < vstart + len {
                return Some(lcn + (vcn - vstart));
            }
        }
        None
    };
    let num_records = mft_total_bytes / mft_record_size as u64;
    log::info!(
        "NTFS fixup: scanning {num_records} MFT records ({} bytes total MFT)",
        mft_total_bytes
    );

    // Step 1: collect MFT entries — ALL non-resident attribute data runs.
    // Each entry will become an `index.runs` slot in the rebuilt index.
    //
    // We build TWO views:
    //   1. `mft_entries: Vec<(lcn, TypedExpect)>` — flat, sorted+deduped by
    //      `(lcn, cc)`. Used by the content-based opaque fallback.
    //   2. `mft_attrs: Vec<Vec<MftFragment>>` — grouped per non-resident
    //      attribute *in MFT-walk order*, preserving emission order. Each
    //      inner Vec is one attribute's fragments (one entry per data run).
    //      This is what gets matched to `stream_attrs` by position.
    struct MftFragment {
        lcn: u64,
        cc: u64,
        magic: Option<ExpectedMagic>,
        expected_vcn: u64,
    }
    let mut mft_entries: Vec<(u64, TypedExpect)> = Vec::new();
    let mut mft_attrs: Vec<Vec<MftFragment>> = Vec::new();
    // Deterministic INDX placement: map (owning_dir_rec, vcn) -> lcn for every
    // $INDEX_ALLOCATION ($I30) fragment. An INDX block in the stream is
    // self-identifying — its index entries carry the parent directory's MFT
    // reference — so we can place each one at exactly the right LCN regardless
    // of cluster-count collisions or stream emission order. Keyed only for the
    // $I30 (directory) index, which is what chkdsk's index checks cover.
    let mut indx_dest: std::collections::HashMap<(u64, u64), u64> =
        std::collections::HashMap::new();
    // Per-FILE $DATA runlist, merged across $ATTRIBUTE_LIST extension records:
    // owning_rec -> Vec<(vcn, lcn, cc)>. A fragmented file's $DATA runs are
    // described across several MFT records, but in the stream they're emitted as
    // one seq-numbered run sequence. Sorting these by vcn reconstructs the
    // file's fragment order, so a sentinel run's seq indexes straight into it.
    // This is what lets sentinel recovery place a hive's interior clusters.
    let mut file_data_frags: std::collections::HashMap<u64, Vec<(u64, u64, u64)>> =
        std::collections::HashMap::new();
    // base record -> file name, for the residual-hole report logged at the end.
    let mut rec_names: std::collections::HashMap<u64, String> = std::collections::HashMap::new();
    // $MFTMirr (rec 1) $DATA — pinned to map onto $MFT's data, never matched to
    // a stream run (Ghost omits the mirror's content; it's a verbatim copy of
    // $MFT's first records). Captured here, re-added at reassembly.
    let mut pinned_mftmirr: Option<(u64, u64)> = None;

    let mut rec_buf = vec![0u8; mft_record_size];
    let mut hint = 0usize;
    for rec_idx in 0..num_records {
        let mft_pos = vbr.mft_cluster * cluster_size + rec_idx * mft_record_size as u64;
        let mut total = 0;
        while total < mft_record_size {
            let n = match read_ntfs_file_aware_into(
                inner,
                index,
                &mut hint,
                compressed,
                mft_pos + total as u64,
                &mut rec_buf[total..],
            ) {
                Ok(n) => n,
                Err(_) => break,
            };
            if n == 0 {
                break;
            }
            total += n;
        }
        if total < mft_record_size || &rec_buf[0..4] != b"FILE" {
            continue;
        }
        let flags = u16::from_le_bytes([rec_buf[22], rec_buf[23]]);
        if flags & 0x01 == 0 {
            continue;
        }
        let base_ref = u64::from_le_bytes([
            rec_buf[0x20],
            rec_buf[0x21],
            rec_buf[0x22],
            rec_buf[0x23],
            rec_buf[0x24],
            rec_buf[0x25],
            rec_buf[0x26],
            rec_buf[0x27],
        ]);
        let base_rec = base_ref & 0x0000_FFFF_FFFF_FFFF;
        let self_rec =
            u32::from_le_bytes([rec_buf[44], rec_buf[45], rec_buf[46], rec_buf[47]]) as u64;
        let owning_rec = if base_rec != 0 { base_rec } else { self_rec };
        // Skip rec 8 ($BadClus, sparse all-volume run) and rec 0 fragment 0
        // (the MFT first run — added separately at the end).
        if self_rec == 8 {
            continue;
        }
        let _ = apply_fixup(&mut rec_buf);
        let attrs = parse_mft_attributes(&rec_buf, vbr.mft_record_size);
        // Capture the file name from a base record's resident $FILE_NAME (0x30)
        // for the residual-hole report. Value layout: parent ref (8) ... name
        // length (1) @0x40, name (UTF-16LE) @0x42.
        if base_ref == 0 {
            for a in &attrs {
                if a.attr_type == 0x30 && a.resident && a.value.len() >= 0x42 {
                    let nl = a.value[0x40] as usize;
                    if 0x42 + nl * 2 <= a.value.len() {
                        let name: String = (0..nl)
                            .filter_map(|i| {
                                char::from_u32(u16::from_le_bytes([
                                    a.value[0x42 + i * 2],
                                    a.value[0x42 + i * 2 + 1],
                                ]) as u32)
                            })
                            .collect();
                        rec_names.entry(self_rec).or_insert(name);
                    }
                    break;
                }
            }
        }
        for attr in &attrs {
            if attr.resident || attr.data_runs.is_empty() {
                continue;
            }
            let magic = ExpectedMagic::from_attr(owning_rec, attr.attr_type);
            // $MFTMirr (rec 1) $DATA: pin it, don't match it to a stream run.
            // Its content is synthesized from $MFT's data (see below).
            if self_rec == 1 && attr.attr_type == 0x80 {
                if pinned_mftmirr.is_none() {
                    if let Some(dr) = attr
                        .data_runs
                        .iter()
                        .find(|dr| dr.cluster_offset > 0 && dr.length > 0)
                    {
                        pinned_mftmirr = Some((dr.cluster_offset as u64, dr.length));
                    }
                }
                continue;
            }
            // Rec 0's $DATA fragment 0 is the MFT first run, set explicitly at
            // scan time. Don't include it as an entry (we'll add it back below).
            let skip_first = self_rec == 0 && attr.attr_type == 0x80;
            let mut vcn = attr.starting_vcn;
            let mut attr_frags: Vec<MftFragment> = Vec::new();
            for (i, dr) in attr.data_runs.iter().enumerate() {
                let len = dr.length;
                if dr.cluster_offset > 0 && len > 0 && !(skip_first && i == 0) {
                    let lcn = dr.cluster_offset as u64;
                    mft_entries.push((
                        lcn,
                        TypedExpect {
                            cc: len,
                            magic,
                            expected_vcn: vcn,
                        },
                    ));
                    attr_frags.push(MftFragment {
                        lcn,
                        cc: len,
                        magic,
                        expected_vcn: vcn,
                    });
                    // Index every $INDEX_ALLOCATION (0xa0) fragment's destination
                    // per VCN. $Secure (rec 9) owns $SII/$SDH which aren't
                    // directory ($I30) indexes; their entries have no parent ref,
                    // so the content lookup just won't match them — excluding
                    // rec 9 keeps the map clean. Each VCN of the attribute maps to
                    // its own LCN (a cc>1 fragment covers several consecutive VCNs).
                    if attr.attr_type == 0xa0 && owning_rec != 9 {
                        for off in 0..len {
                            indx_dest.insert((owning_rec, vcn + off), lcn + off);
                        }
                    }
                    // Accumulate $DATA fragments per owning file (skip $MFT,
                    // rec 0, served separately) for sentinel recovery.
                    if attr.attr_type == 0x80 && owning_rec != 0 {
                        file_data_frags
                            .entry(owning_rec)
                            .or_default()
                            .push((vcn, lcn, len));
                    }
                }
                vcn += len;
            }
            if !attr_frags.is_empty() {
                mft_attrs.push(attr_frags);
            }
        }
    }

    // Dedupe MFT entries by (lcn, cc, magic, expected_vcn). $ATTRIBUTE_LIST can
    // cause the same attribute to surface from both the base record and an
    // extension; we only want one entry per (lcn, cc) pair.
    mft_entries.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.cc.cmp(&b.1.cc))
            .then(a.1.expected_vcn.cmp(&b.1.expected_vcn))
    });
    mft_entries.dedup_by(|a, b| a.0 == b.0 && a.1.cc == b.1.cc);
    log::info!(
        "NTFS fixup: {} unique MFT entries (after dedup)",
        mft_entries.len()
    );

    // Step 2: observe every existing run's actual content (peek at file_offset).
    // Skip the MFT run itself — it's preserved separately.
    #[derive(Debug, Clone, Copy)]
    struct ObservedRun {
        file_offset: u64,
        cc: u64,
        kind: Option<ExpectedMagic>,
        self_vcn: Option<u64>,
        /// Stream-side fragment index captured during the lazy scan. `u64::MAX`
        /// for entries whose seq wasn't recorded (uncompressed path TODO).
        seq: u64,
    }
    let mut observed_runs: Vec<ObservedRun> = Vec::with_capacity(index.runs.len());
    let mut order: Vec<usize> = (0..index.runs.len()).collect();
    order.sort_by_key(|&i| index.runs[i].file_offset);
    for run_i in order {
        let r = &index.runs[run_i];
        if r.lcn_start == vbr.mft_cluster {
            continue;
        }
        // Skip the synthesized $MFTMirr run: it shares the MFT's file_offset
        // (it maps onto $MFT's data) and would otherwise be observed as a
        // phantom FILE-magic run and mis-matched. It's re-added at reassembly.
        if r.file_offset == mft_run.file_offset {
            continue;
        }
        let mut buf = [0u8; 24];
        let got = peek_run_bytes(inner, compressed.as_mut(), r.file_offset, &mut buf);
        if got < 4 {
            continue;
        }
        let kind = ExpectedMagic::from_observed(&[buf[0], buf[1], buf[2], buf[3]]);
        let self_vcn = if kind == Some(ExpectedMagic::Indx) && got >= 0x18 {
            Some(u64::from_le_bytes([
                buf[0x10], buf[0x11], buf[0x12], buf[0x13], buf[0x14], buf[0x15], buf[0x16],
                buf[0x17],
            ]))
        } else {
            None
        };
        observed_runs.push(ObservedRun {
            file_offset: r.file_offset,
            cc: r.cluster_count,
            kind,
            self_vcn,
            seq: r.seq,
        });
    }
    // observed_runs is now in stream order (sorted by file_offset). Group it
    // into stream-attributes by seq=0 boundaries: a new attribute starts every
    // time seq drops back to 0. Each group is one MFT non-resident attribute's
    // fragment list in emission order.
    let mut stream_attrs: Vec<Vec<usize>> = Vec::new(); // groups of observed_runs indices
    {
        let mut current: Vec<usize> = Vec::new();
        for (i, o) in observed_runs.iter().enumerate() {
            if o.seq == 0 {
                if !current.is_empty() {
                    stream_attrs.push(std::mem::take(&mut current));
                }
                current.push(i);
            } else if o.seq == u64::MAX {
                // Uncompressed-path sentinel — flush as a standalone group so
                // it still gets matched by fragment-shape (which is just [cc]).
                if !current.is_empty() {
                    stream_attrs.push(std::mem::take(&mut current));
                }
                stream_attrs.push(vec![i]);
            } else {
                current.push(i);
            }
        }
        if !current.is_empty() {
            stream_attrs.push(current);
        }
    }
    log::info!(
        "NTFS fixup: grouped {} observed runs into {} stream-attributes",
        observed_runs.len(),
        stream_attrs.len()
    );
    let mut indx_obs = 0;
    let mut file_obs = 0;
    let mut log_obs = 0;
    let mut sd_obs = 0;
    let mut other_obs = 0;
    for o in &observed_runs {
        match o.kind {
            Some(ExpectedMagic::Indx) => indx_obs += 1,
            Some(ExpectedMagic::File) => file_obs += 1,
            Some(ExpectedMagic::Logfile) => log_obs += 1,
            Some(ExpectedMagic::SecurityDescriptor) => sd_obs += 1,
            None => other_obs += 1,
        }
    }
    log::info!(
        "NTFS fixup: {} runs observed (INDX={} FILE={} LOGFILE={} SD={} other={})",
        observed_runs.len(),
        indx_obs,
        file_obs,
        log_obs,
        sd_obs,
        other_obs
    );

    // Step 3: assign observed runs to MFT entries.
    //
    // Pass 0 (seq-based) — the deterministic path. `stream_attrs[k]` is the
    // k-th attribute Ghost emitted; `mft_attrs[k]` is the k-th non-resident
    // attribute we found walking the MFT. Their fragments line up
    // one-to-one IF the emission order matches MFT-walk order AND the
    // fragment counts agree. When a mismatch is detected (different fragment
    // count or magic disagreement on the first fragment) we BAIL on seq
    // matching for that attribute and let Pass 1 / Pass 2 handle its runs as
    // before — that's the fallback for `$ATTRIBUTE_LIST`-reordered streams
    // or any other emission-order surprise.
    //
    // Pass 1 — magic-typed entries (content-based fallback). For each MFT
    // entry with a magic prediction (INDX/FILE/LOGFILE), find an unused
    // observed run whose cc + kind + (for INDX) self_vcn match.
    //
    // Pass 2 — opaque entries (user $DATA, $Secure $DATA $SDS, etc.).
    // Preserve the matcher's original `(lcn, cc) → file_offset` choice.
    let mut used = vec![false; observed_runs.len()];
    let mut assignments: Vec<(u64, u64, u64, u64)> = Vec::new(); // (lcn, cc, file_offset, seq)
    let mut typed_matched = 0usize;
    let mut typed_unmatched = 0usize;
    let mut seq_matched = 0usize;
    let mut seq_bailouts = 0usize;
    // (lcn, cc) pairs already assigned, shared across all passes so none
    // double-assign. Maintained incrementally from the first (INDX-content) pass
    // onward.
    let mut claimed: std::collections::HashSet<(u64, u64)> = std::collections::HashSet::new();

    // Pass -1 (INDX content): deterministic directory-index placement, run
    // before everything else. Every $I30 INDX block names its parent directory
    // in its first index entry's $FILE_NAME key; combined with the block's
    // self-VCN that uniquely identifies the destination LCN (`indx_dest`),
    // independent of cluster-count collisions and stream emission order — the
    // failure mode that desynced positional seq-matching on large volumes and
    // produced chkdsk "index entry ... is incorrect" / "incorrectly sorted"
    // errors en masse. INDX blocks that can't be identified (empty nodes, or
    // non-$I30 indexes whose keys aren't $FILE_NAME) fall through to the later
    // passes unchanged.
    let mut indx_content_matched = 0usize;
    {
        let mut blk = vec![0u8; 8192];
        for i in 0..observed_runs.len() {
            if used[i] || observed_runs[i].kind != Some(ExpectedMagic::Indx) {
                continue;
            }
            let fo = observed_runs[i].file_offset;
            let got = peek_run_bytes(inner, compressed.as_mut(), fo, &mut blk);
            if got < 0x20 || &blk[0..4] != b"INDX" {
                continue;
            }
            let self_vcn = u64::from_le_bytes(blk[0x10..0x18].try_into().unwrap());
            // INDEX_NODE_HEADER sits at 0x18; its first-entry offset is relative
            // to the header start (0x18).
            let first_off = 0x18 + u32::from_le_bytes(blk[0x18..0x1C].try_into().unwrap()) as usize;
            if first_off + 0x18 > got {
                continue;
            }
            let entry_flags = u16::from_le_bytes([blk[first_off + 0x0C], blk[first_off + 0x0D]]);
            let key_len =
                u16::from_le_bytes([blk[first_off + 0x0A], blk[first_off + 0x0B]]) as usize;
            // 0x02 = last/end entry. With no key it's an empty node — unidentifiable.
            if entry_flags & 0x02 != 0 || key_len < 8 {
                continue;
            }
            // The key is a $FILE_NAME; its first 8 bytes are the parent dir ref.
            let pr_off = first_off + 0x10;
            if pr_off + 8 > got {
                continue;
            }
            let parent_ref = u64::from_le_bytes(blk[pr_off..pr_off + 8].try_into().unwrap())
                & 0x0000_FFFF_FFFF_FFFF;
            if let Some(&lcn) = indx_dest.get(&(parent_ref, self_vcn)) {
                let cc = observed_runs[i].cc;
                if !claimed.contains(&(lcn, cc)) {
                    used[i] = true;
                    assignments.push((lcn, cc, fo, observed_runs[i].seq));
                    claimed.insert((lcn, cc));
                    indx_content_matched += 1;
                }
            }
        }
    }
    log::info!(
        "NTFS fixup: Pass -1 (INDX content) — {indx_content_matched} directory-index blocks placed"
    );

    // Pass -1b (MFT-fragment content): place FILE-magic runs — the $MFT's own
    // later $DATA fragments — deterministically. Each fragment is a sequence of
    // FILE records, and every record stores its own record number at offset
    // 0x2C. The first record's number fixes the fragment's $MFT VCN, which the
    // $MFT $DATA runs map to an LCN. This avoids the cc-based ambiguity that
    // otherwise swaps same-size MFT fragments and leaves a few records reading
    // as zeros / foreign data ("Attribute record (80) is corrupt" in chkdsk).
    let mut mft_frag_matched = 0usize;
    {
        let mut hdr = [0u8; 0x30];
        for i in 0..observed_runs.len() {
            if used[i] || observed_runs[i].kind != Some(ExpectedMagic::File) {
                continue;
            }
            let fo = observed_runs[i].file_offset;
            let got = peek_run_bytes(inner, compressed.as_mut(), fo, &mut hdr);
            if got < 0x30 || &hdr[0..4] != b"FILE" {
                continue;
            }
            // Record number this fragment starts at (offset 0x2C).
            let first_rec = u32::from_le_bytes([hdr[0x2C], hdr[0x2D], hdr[0x2E], hdr[0x2F]]) as u64;
            // VCN of that record within $MFT, then its LCN via the $MFT runs.
            let vcn = first_rec * mft_record_size as u64 / cluster_size;
            if let Some(lcn) = mft_vcn_to_lcn(vcn) {
                let cc = observed_runs[i].cc;
                if !claimed.contains(&(lcn, cc)) {
                    used[i] = true;
                    assignments.push((lcn, cc, fo, observed_runs[i].seq));
                    claimed.insert((lcn, cc));
                    mft_frag_matched += 1;
                }
            }
        }
    }
    log::info!(
        "NTFS fixup: Pass -1b (MFT-fragment content) — {mft_frag_matched} $MFT fragments placed"
    );

    // Pass 0: seq-based attribute alignment.
    let n_pairs = stream_attrs.len().min(mft_attrs.len());
    for k in 0..n_pairs {
        let s_idxs = &stream_attrs[k];
        let mft_frags = &mft_attrs[k];
        // Skip groups whose runs or destinations the INDX-content pass already
        // claimed — re-committing them here would double-assign.
        if s_idxs.iter().any(|&si| used[si])
            || mft_frags.iter().any(|f| claimed.contains(&(f.lcn, f.cc)))
        {
            continue;
        }
        // Bail if the shapes don't agree — different fragment counts or
        // cc mismatch on a fragment means Ghost's emission order isn't what
        // we assumed for this attribute.
        if s_idxs.len() != mft_frags.len() {
            seq_bailouts += 1;
            continue;
        }
        let mut shape_ok = true;
        for (j, &si) in s_idxs.iter().enumerate() {
            if observed_runs[si].cc != mft_frags[j].cc {
                shape_ok = false;
                break;
            }
            // Magic disagreement on a fragment whose attribute predicts a
            // specific magic is a strong "this isn't the right attribute"
            // signal. Check fragment 0 (others may genuinely lack magic).
            if j == 0 {
                if let Some(want_m) = mft_frags[j].magic {
                    if observed_runs[si].kind != Some(want_m) {
                        shape_ok = false;
                        break;
                    }
                    if want_m == ExpectedMagic::Indx
                        && observed_runs[si].self_vcn != Some(mft_frags[j].expected_vcn)
                    {
                        shape_ok = false;
                        break;
                    }
                }
            }
        }
        if !shape_ok {
            seq_bailouts += 1;
            continue;
        }
        // Shapes agree — commit each fragment.
        for (j, &si) in s_idxs.iter().enumerate() {
            used[si] = true;
            assignments.push((
                mft_frags[j].lcn,
                mft_frags[j].cc,
                observed_runs[si].file_offset,
                observed_runs[si].seq,
            ));
            claimed.insert((mft_frags[j].lcn, mft_frags[j].cc));
            seq_matched += 1;
        }
    }
    log::info!(
        "NTFS fixup: Pass 0 (seq) — {} fragments matched, {} attrs bailed out",
        seq_matched,
        seq_bailouts
    );

    // Pass 1: typed entries that Pass 0 didn't already claim.
    for (lcn, t) in &mft_entries {
        if claimed.contains(&(*lcn, t.cc)) {
            continue;
        }
        let Some(want_magic) = t.magic else {
            continue;
        };
        let mut found: Option<usize> = None;
        for (i, o) in observed_runs.iter().enumerate() {
            if used[i] {
                continue;
            }
            if o.cc != t.cc {
                continue;
            }
            if o.kind != Some(want_magic) {
                continue;
            }
            if want_magic == ExpectedMagic::Indx && o.self_vcn != Some(t.expected_vcn) {
                continue;
            }
            found = Some(i);
            break;
        }
        if let Some(i) = found {
            used[i] = true;
            assignments.push((
                *lcn,
                t.cc,
                observed_runs[i].file_offset,
                observed_runs[i].seq,
            ));
            claimed.insert((*lcn, t.cc));
            typed_matched += 1;
        } else {
            typed_unmatched += 1;
        }
    }

    // Pass 2: opaque entries — preserve the matcher's original choice.
    let mut original_assignments: std::collections::HashMap<(u64, u64), u64> =
        std::collections::HashMap::with_capacity(index.runs.len());
    for r in &index.runs {
        original_assignments
            .entry((r.lcn_start, r.cluster_count))
            .or_insert(r.file_offset);
    }
    let mut opaque_matched = 0usize;
    let mut opaque_unmatched = 0usize;
    for (lcn, t) in &mft_entries {
        if t.magic.is_some() {
            continue;
        }
        if claimed.contains(&(*lcn, t.cc)) {
            continue;
        }
        if let Some(&fo) = original_assignments.get(&(*lcn, t.cc)) {
            assignments.push((*lcn, t.cc, fo, u64::MAX));
            claimed.insert((*lcn, t.cc));
            opaque_matched += 1;
        } else {
            opaque_unmatched += 1;
        }
    }

    // Also preserve original placement for any TYPED entries that Pass 1
    // couldn't match — better to keep the matcher's guess than to lose the run
    // entirely (would read as zeros and chkdsk would think the file is empty).
    for (lcn, t) in &mft_entries {
        if t.magic.is_none() {
            continue;
        }
        if claimed.contains(&(*lcn, t.cc)) {
            continue;
        }
        if let Some(&fo) = original_assignments.get(&(*lcn, t.cc)) {
            assignments.push((*lcn, t.cc, fo, u64::MAX));
            claimed.insert((*lcn, t.cc));
        }
    }

    // Sentinel recovery. The lazy scan records runs it can't assign an LCN with
    // lcn_start == u64::MAX (it found the run header + decoded the data, but had
    // no candidate (lcn,cc) at scan time — typical for a fragment of a file
    // whose runlist is described in an $ATTRIBUTE_LIST extension record). Their
    // data IS present; only the LCN is missing. Recover each via its
    // stream-attribute neighbours: within a seq-delimited stream attribute, an
    // already-placed (matched) run identifies the owning MFT attribute, and the
    // sentinel is just another fragment of it — its LCN is mft_attrs[k][seq].
    // This is what restores a registry hive's interior $DATA clusters that the
    // scan dropped, without the cc-collision risk of blind matching.
    let orig_lcn_by_fo: std::collections::HashMap<u64, u64> = index
        .runs
        .iter()
        .map(|r| (r.file_offset, r.lcn_start))
        .collect();
    // Finalize per-file runlists: sort each file's $DATA fragments by VCN (=
    // stream emission order) and index every fragment lcn back to its file +
    // position. Position is the fragment's index in the sorted runlist, which
    // equals its `seq` offset from any sibling fragment in the same stream run.
    let mut file_runlist: std::collections::HashMap<u64, Vec<(u64, u64)>> =
        std::collections::HashMap::with_capacity(file_data_frags.len()); // rec -> [(lcn, cc)]
    let mut lcn_to_file_pos: std::collections::HashMap<u64, (u64, usize)> =
        std::collections::HashMap::new(); // lcn -> (owning_rec, index)
    for (rec, mut frags) in file_data_frags.into_iter() {
        frags.sort_by_key(|&(vcn, _, _)| vcn);
        let runs: Vec<(u64, u64)> = frags.iter().map(|&(_, l, c)| (l, c)).collect();
        for (i, &(l, _)) in runs.iter().enumerate() {
            lcn_to_file_pos.entry(l).or_insert((rec, i));
        }
        file_runlist.insert(rec, runs);
    }
    // Walk observed runs in stream (file_offset) emission order. Ghost emits a
    // file's $DATA fragments consecutively, in VCN order, so a matched run
    // identifies the current file + its position in that file's runlist, and the
    // run immediately following is the next fragment. A sentinel sandwiched
    // between a file's matched runs is therefore that file's next runlist entry —
    // recovered with no dependence on the (irregular) seq field. The cc check +
    // anchor reset on any mismatch keep a wrong guess from landing.
    let mut sentinel_recovered = 0usize;
    let mut cur: Option<(u64, usize)> = None; // (owning_rec, next runlist index)
    for (i, o) in observed_runs.iter().enumerate() {
        let olcn = orig_lcn_by_fo
            .get(&o.file_offset)
            .copied()
            .unwrap_or(u64::MAX);
        if olcn != u64::MAX {
            // Matched run: (re)anchor to the file + fragment AFTER this one.
            cur = lcn_to_file_pos.get(&olcn).map(|&(rec, idx)| (rec, idx + 1));
            continue;
        }
        // Sentinel: place it at the current file's next expected fragment.
        if used[i] {
            continue;
        }
        let Some((rec, next_idx)) = cur else { continue };
        let placed = file_runlist
            .get(&rec)
            .and_then(|runs| runs.get(next_idx).copied())
            .filter(|&(lcn, cc)| cc == o.cc && !claimed.contains(&(lcn, cc)));
        match placed {
            Some((lcn, cc)) => {
                assignments.push((lcn, cc, o.file_offset, o.seq));
                claimed.insert((lcn, cc));
                used[i] = true;
                sentinel_recovered += 1;
                cur = Some((rec, next_idx + 1));
            }
            None => cur = None, // shape disagreed — stop trusting this anchor
        }
    }
    log::info!(
        "NTFS fixup: sentinel recovery placed {sentinel_recovered} previously-unmapped runs"
    );

    // Safety net: preserve every observed run the passes above couldn't match
    // to an MFT entry, at its ORIGINAL lazy-scan LCN. The rebuild must never
    // DROP a run the lazy scan found — dropping reads it back as zeros. This
    // happens when a file's $DATA-describing attribute lives in an extension
    // record we couldn't read (a zeroed MFT cluster), so the MFT walk never
    // collected that file's runs and nothing here matches its observed stream
    // run. Large registry hives hit exactly this — losing their first $DATA
    // cluster (the `regf` base block) made the hive unloadable. Keep the lazy
    // placement unless its LCN was already reassigned to a corrected run.
    let mut preserved_unmatched = 0usize;
    for (i, o) in observed_runs.iter().enumerate() {
        if used[i] {
            continue;
        }
        let Some(&orig_lcn) = orig_lcn_by_fo.get(&o.file_offset) else {
            continue;
        };
        if claimed.contains(&(orig_lcn, o.cc)) {
            continue;
        }
        assignments.push((orig_lcn, o.cc, o.file_offset, o.seq));
        claimed.insert((orig_lcn, o.cc));
        used[i] = true;
        preserved_unmatched += 1;
    }

    log::info!(
        "NTFS fixup: typed-matched={typed_matched} typed-unmatched={typed_unmatched} \
         opaque-matched={opaque_matched} opaque-unmatched={opaque_unmatched} \
         preserved-unmatched={preserved_unmatched} unused-observed={}",
        used.iter().filter(|&&u| !u).count()
    );

    // Step 4: rebuild index.runs. Preserve the MFT run unchanged; replace
    // everything else with the new assignments.
    let mut new_runs: Vec<NtfsGhoClusterRun> = Vec::with_capacity(assignments.len() + 2);
    let mft_file_offset = mft_run.file_offset;
    let mft_run_clusters = mft_run.cluster_count;
    new_runs.push(mft_run);
    // Re-add the synthesized $MFTMirr: clusters mapped onto the start of $MFT's
    // data so the mirror reads as a verbatim copy of $MFT's first records.
    if let Some((mirr_lcn, mirr_cc)) = pinned_mftmirr {
        let cc = mirr_cc.min(mft_run_clusters);
        if cc > 0 {
            new_runs.push(NtfsGhoClusterRun {
                lcn_start: mirr_lcn,
                cluster_count: cc,
                file_offset: mft_file_offset,
                seq: 0,
            });
            log::info!(
                "NTFS fixup: re-added synthesized $MFTMirr ({} cluster(s) at lcn {})",
                cc,
                mirr_lcn
            );
        }
    }
    for (lcn, cc, file_offset, seq) in assignments {
        new_runs.push(NtfsGhoClusterRun {
            lcn_start: lcn,
            cluster_count: cc,
            file_offset,
            seq,
        });
    }
    new_runs.sort_by_key(|r| r.lcn_start);
    log::info!(
        "NTFS fixup: rebuilt index, {} runs (was {})",
        new_runs.len(),
        index.runs.len()
    );
    index.runs = new_runs;

    // Residual-hole report. After recovery, any file $DATA cluster the final
    // run set still doesn't cover reads back as zeros in the exported image —
    // genuinely-uncaptured data (Ghost-skipped transient files like
    // hiberfil/pagefile, or runs whose headers the scan never found). These
    // are harmless to mount but represent incomplete file content, so list the
    // worst offenders in the log as a record of what couldn't be restored.
    {
        let mut covered: Vec<(u64, u64)> = index
            .runs
            .iter()
            .filter(|r| r.lcn_start != u64::MAX)
            .map(|r| (r.lcn_start, r.cluster_count))
            .collect();
        covered.sort_by_key(|r| r.0);
        let is_covered = |lcn: u64| -> bool {
            covered
                .binary_search_by(|&(s, l)| {
                    if lcn < s {
                        std::cmp::Ordering::Greater
                    } else if lcn >= s + l {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
                .is_ok()
        };
        let mut holes: Vec<(u64, u64)> = Vec::new(); // (owning_rec, uncovered clusters)
        let mut total_hole = 0u64;
        for (rec, runs) in &file_runlist {
            let mut miss = 0u64;
            for &(lcn, cc) in runs {
                for c in 0..cc {
                    if !is_covered(lcn + c) {
                        miss += 1;
                    }
                }
            }
            if miss > 0 {
                holes.push((*rec, miss));
                total_hole += miss;
            }
        }
        if !holes.is_empty() {
            holes.sort_by_key(|b| std::cmp::Reverse(b.1));
            log::warn!(
                "NTFS: {} file(s) have zero-filled $DATA holes (no captured data); \
                 {} clusters total. These files are incomplete in the exported image:",
                holes.len(),
                total_hole
            );
            for (rec, miss) in holes.iter().take(40) {
                let name = rec_names
                    .get(rec)
                    .map(|s| s.as_str())
                    .unwrap_or("<name in extension record>");
                log::warn!("  rec#{rec} ({miss} clusters zero-filled): {name}");
            }
            if holes.len() > 40 {
                log::warn!("  ... and {} more file(s)", holes.len() - 40);
            }
        }
    }
}

/// Recover `$Boot`'s first 16 sectors by content-matching the VBR.
///
/// The read path synthesizes the boot region (sector 0 = VBR, rest zeros) to
/// keep partition-detection probes cheap, so the real NTFS boot code (sectors
/// 1-15, the bootstrap that loads NTLDR) is otherwise dropped. Ghost does store
/// `$Boot`, and `$Boot`'s sector 0 is byte-identical to the volume VBR, so the
/// `$Boot` run is the one whose first 512 bytes equal the VBR. Matching by
/// content (rather than trusting the cluster-count LCN mapping) is robust to any
/// run-mapping ambiguity. Runs are checked in stream order (`$Boot` is stored
/// early) and the search stops at the first match. Returns up to 8 KiB (16
/// sectors) of boot code, or `None` if no match (caller falls back to zeros).
///
/// Requires a complete run index (call after the lazy scan finishes).
pub(super) fn recover_ntfs_boot_code(
    inner: &mut SpanReader,
    index: &NtfsGhoIndex,
    compressed: &mut Option<NtfsCompressedState>,
) -> Option<Vec<u8>> {
    const BOOT_BYTES: usize = 16 * 512;
    let mut order: Vec<&NtfsGhoClusterRun> = index.runs.iter().collect();
    order.sort_by_key(|r| r.file_offset);

    let mut head = [0u8; 512];
    for run in order {
        let read_head = |inner: &mut SpanReader,
                         compressed: &mut Option<NtfsCompressedState>,
                         head: &mut [u8; 512]|
         -> bool {
            if let Some(cs) = compressed.as_mut() {
                let mut rdr = NtfsDecompressingReader {
                    inner,
                    state: cs,
                    position: run.file_offset,
                };
                rdr.read_exact(head).is_ok()
            } else {
                inner.seek(SeekFrom::Start(run.file_offset)).is_ok()
                    && inner.read_exact(head).is_ok()
            }
        };
        if !read_head(inner, compressed, &mut head) {
            continue;
        }
        if head == index.vbr {
            let avail = (run.cluster_count * index.cluster_size) as usize;
            let want = BOOT_BYTES.min(avail);
            let mut buf = vec![0u8; want];
            let ok = if let Some(cs) = compressed.as_mut() {
                let mut rdr = NtfsDecompressingReader {
                    inner,
                    state: cs,
                    position: run.file_offset,
                };
                rdr.read_exact(&mut buf).is_ok()
            } else {
                inner.seek(SeekFrom::Start(run.file_offset)).is_ok()
                    && inner.read_exact(&mut buf).is_ok()
            };
            if ok {
                return Some(buf);
            }
        }
    }
    None
}

/// Bytes per sector from a parsed NTFS VBR (offset 0x0B), defaulting to 512
/// when the field is zero/garbage.
pub(super) fn ntfs_vbr_sector_size(vbr: &[u8; 512]) -> u64 {
    let s = u16::from_le_bytes([vbr[11], vbr[12]]) as u64;
    if s == 0 {
        512
    } else {
        s
    }
}

/// Total served length of the reconstructed NTFS volume in bytes.
///
/// `index.volume_size` is the NTFS-addressable volume (`total_sectors * bps`),
/// which deliberately EXCLUDES the final sector. NTFS places a copy of the VBR
/// (the backup boot sector) in that excluded final sector, so the real volume —
/// and therefore the partition we export it into — is one sector longer. The
/// read path serves that trailing sector from the VBR (see the backup-boot-sector
/// branch in `read_ntfs_file_aware_into`); without the `+1` the exported volume
/// is a sector short and the backup boot sector lands at the wrong offset, which
/// chkdsk and the Windows boot path both flag.
pub(super) fn ntfs_served_size(index: &NtfsGhoIndex) -> u64 {
    index.volume_size + ntfs_vbr_sector_size(&index.vbr)
}

/// Read bytes from an NTFS file-aware GHO at `position` in the logical
/// volume. Uses binary search over the sorted cluster run index.
pub(super) fn read_ntfs_file_aware_into(
    inner: &mut SpanReader,
    index: &mut NtfsGhoIndex,
    last_run_hint: &mut usize,
    compressed: &mut Option<NtfsCompressedState>,
    position: u64,
    out: &mut [u8],
) -> std::io::Result<usize> {
    if out.is_empty() {
        return Ok(0);
    }

    let cluster_size = index.cluster_size;
    let lcn = position / cluster_size;
    let offset_in_cluster = (position % cluster_size) as usize;
    let avail_in_cluster = cluster_size as usize - offset_in_cluster;
    let mut to_read = out.len().min(avail_in_cluster);

    // NTFS mirrors the VBR in the backup boot sector, the ONE sector that sits
    // immediately past the addressable volume (`total_sectors` excludes it; see
    // `ntfs_served_size`). It isn't part of any stored run. Clamp normal reads so
    // they stop at that boundary; the read that then starts there serves the VBR
    // copy below. Without the clamp a sequential read straddling the boundary
    // would serve the whole (unmapped) tail as zeros, blanking the backup sector.
    let sector_size = ntfs_vbr_sector_size(&index.vbr);
    let backup_start = index.volume_size;
    if position < backup_start && position + to_read as u64 > backup_start {
        to_read = (backup_start - position) as usize;
    }

    // $Boot region (first 16 sectors, LCN 0). Sector 0 is always the
    // authoritative GHPR-embedded VBR (this also keeps partition-detection
    // probes cheap). Sectors 1-15 are the NTFS boot code: served from
    // `index.boot_code` when an export prepared it (recover_ntfs_boot_code),
    // else zeros — the volume still mounts, just won't self-boot.
    const SYNTH_BOOT_BYTES: u64 = 16 * 512; // 16 sectors = RDB scan range
    if position < SYNTH_BOOT_BYTES {
        let pos = position as usize;
        let n = to_read.min(SYNTH_BOOT_BYTES as usize - pos);
        for (i, b) in out[..n].iter_mut().enumerate() {
            let fo = pos + i;
            *b = if fo < 512 {
                index.vbr[fo]
            } else {
                index.boot_code.get(fo).copied().unwrap_or(0)
            };
        }
        return Ok(n);
    }

    // Backup boot sector: serve the VBR copy for the trailing sector at
    // `volume_size` (synthesized from the GHPR VBR; see the clamp above). The
    // served volume runs to `volume_size + sector_size` (`ntfs_served_size`), so
    // this is the genuine last sector of the partition — where NTFS expects it.
    if position >= backup_start && position < index.volume_size + sector_size {
        let in_sector = (position - backup_start) as usize;
        let n = to_read.min(sector_size as usize - in_sector);
        for (i, b) in out[..n].iter_mut().enumerate() {
            *b = index.vbr.get(in_sector + i).copied().unwrap_or(0);
        }
        return Ok(n);
    }

    // Serve the synthesized $Bitmap. Ghost omits $Bitmap content from the
    // backup, so its LCNs have no mapped run and would otherwise read as zeros
    // (an all-free bitmap → strict NTFS drivers reject the volume). Serve the
    // bitmap we build from the complete run set instead.
    if index
        .bitmap_runs
        .iter()
        .any(|&(l, c)| lcn >= l && lcn < l + c)
    {
        // Build it on first access. For the compressed path the run set isn't
        // complete until the lazy scan finishes, so force it to EOF first.
        if index.synth_bitmap.is_empty() {
            if let Some(cs) = compressed.as_mut() {
                if cs.lazy_scan.is_some() {
                    extend_ntfs_compressed_index(inner, index, cs, u64::MAX)?;
                }
            }
            let total_clusters = index.volume_size / index.cluster_size;
            let mut allocation: Vec<(u64, u64)> = index
                .runs
                .iter()
                .map(|r| (r.lcn_start, r.cluster_count))
                .collect();
            allocation.extend_from_slice(&index.bitmap_runs);
            index.synth_bitmap = build_synth_ntfs_bitmap(&allocation, total_clusters);
            log::info!(
                "Synthesized $Bitmap on demand: {} bytes from {} runs for {} clusters",
                index.synth_bitmap.len(),
                index.runs.len(),
                total_clusters,
            );
            // The scan is now complete and the bitmap built; refresh the cache
            // so subsequent opens also reuse the synthesized $Bitmap.
            if let Some(cs) = compressed.as_ref() {
                if let Some(key) = cs.cache_key.clone() {
                    ntfs_scan_cache_store(&key, index, cs);
                }
            }
        }
        let mut base = 0u64; // cluster offset within the $Bitmap file
        for &(blcn, bcnt) in &index.bitmap_runs {
            if lcn >= blcn && lcn < blcn + bcnt {
                let file_off =
                    ((base + (lcn - blcn)) * cluster_size + offset_in_cluster as u64) as usize;
                for (i, b) in out[..to_read].iter_mut().enumerate() {
                    *b = index.synth_bitmap.get(file_off + i).copied().unwrap_or(0);
                }
                return Ok(to_read);
            }
            base += bcnt;
        }
    }

    let find_run = |runs: &[NtfsGhoClusterRun], hint: usize| -> Option<usize> {
        if hint < runs.len() {
            let h = &runs[hint];
            if lcn >= h.lcn_start && lcn < h.lcn_start + h.cluster_count {
                return Some(hint);
            }
        }
        runs.binary_search_by(|r| {
            if lcn < r.lcn_start {
                std::cmp::Ordering::Greater
            } else if lcn >= r.lcn_start + r.cluster_count {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .ok()
    };

    let mut found_idx = find_run(&index.runs, *last_run_hint);

    if found_idx.is_none() {
        if let Some(cs) = compressed.as_mut() {
            if let Some(scan) = &cs.lazy_scan {
                // Only trigger the expensive forward scan if this LCN is
                // actually in the MFT data-run queue (= allocated cluster).
                // Partition detection probes random sectors and should get
                // zeros, not trigger a full decompression pass.
                let is_known_lcn = scan
                    .data_run_queue
                    .iter()
                    .any(|&(l, c)| lcn >= l && lcn < l + c);
                if is_known_lcn {
                    extend_ntfs_compressed_index(inner, index, cs, lcn)?;
                    found_idx = find_run(&index.runs, *last_run_hint);
                }
            }
        }
    }

    let Some(idx) = found_idx else {
        for b in &mut out[..to_read] {
            *b = 0;
        }
        return Ok(to_read);
    };

    *last_run_hint = idx;
    let run = &index.runs[idx];
    let cluster_in_run = lcn - run.lcn_start;
    let file_offset = run.file_offset + cluster_in_run * cluster_size + offset_in_cluster as u64;

    if let Some(cs) = compressed {
        // A mapped run whose decompressed data wasn't recorded (file_offset at
        // or past the end of the decompressed stream) would make the reader
        // return 0, which sequential consumers (e.g. whole-disk export) treat
        // as EOF and stop early. Zero-fill the cluster instead so reads keep
        // advancing, and log it so the mismapping can be investigated.
        if file_offset >= cs.total_decompressed {
            log::warn!(
                "GHO NTFS: run for lcn {lcn} maps to decompressed offset {file_offset} \
                 >= total_decompressed {} (run lcn_start={} cluster_count={} file_offset={}); \
                 zero-filling cluster",
                cs.total_decompressed,
                run.lcn_start,
                run.cluster_count,
                run.file_offset,
            );
            for b in &mut out[..to_read] {
                *b = 0;
            }
            return Ok(to_read);
        }
        let mut reader = NtfsDecompressingReader {
            inner,
            state: cs,
            position: file_offset,
        };
        return reader.read(&mut out[..to_read]);
    }

    inner
        .seek(SeekFrom::Start(file_offset))
        .map_err(disk_core::io_other)?;
    let n = inner.read(&mut out[..to_read])?;
    Ok(n)
}
