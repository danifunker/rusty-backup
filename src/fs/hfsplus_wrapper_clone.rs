//! Wrapper-aware HFS+ defrag clone.
//!
//! Classic Mac OS 8.x/9.x volumes store an HFS+ volume **embedded inside an
//! HFS wrapper**: the outer Apple_HFS partition is itself a classic HFS
//! volume whose MDB has `drEmbedSigWord == 0x482B`/`0x4858`, with
//! `drEmbedExtent` pointing at a contiguous range of outer allocation
//! blocks that hold a real HFS+ volume. Mac OS 8.1+ reads the MDB, jumps
//! straight to the inner HFS+, and mounts it; pre-8.1 systems mount the
//! outer HFS and see a "Where_have_all_my_files_gone?" read-me file.
//!
//! [`stream_defragmented_hfsplus`](crate::fs::hfsplus_defrag::stream_defragmented_hfsplus)
//! only handles **flat** HFS+ (no wrapper). [`stream_wrapped_defragmented_hfsplus`]
//! in this module wraps that with the surrounding HFS shell:
//!
//! ```text
//! +---- new wrapper partition ----+
//! | boot blocks (verbatim)        |
//! | primary MDB (patched)         |
//! | volume bitmap (patched)       |
//! | wrapper alloc blocks 0..S-1   |  (verbatim from source — holds the
//! |   ("read-me" file + B-trees)  |   wrapper's catalog/extents, untouched)
//! | embedded extent (S..S+N-1)    |  ← stream_defragmented_hfsplus output
//! |   = new flat HFS+ volume      |
//! | alt MDB (patched mirror)      |
//! | reserved last sector          |
//! +-------------------------------+
//! ```
//!
//! where `S = drEmbedExtent.startBlock` (unchanged from source) and `N =
//! ceil(target_inner_size / drAlBlkSiz)` (smaller than the source's
//! embedded extent). `drNmAlBlks` shrinks to `S + N` and the wrapper's
//! internal catalog/extents B-trees are preserved byte-for-byte because
//! their allocation blocks all lie in `[0..S)` and stay at the same
//! source byte offsets.

use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use crate::fs::filesystem::FilesystemError;
use crate::fs::hfsplus::HfsPlusFilesystem;
use crate::fs::hfsplus_defrag::{stream_defragmented_hfsplus, DefragReport};

/// Fields parsed from the outer HFS wrapper MDB that the clone pipeline
/// needs to rebuild the wrapper around a shrunken inner volume.
#[derive(Debug, Clone)]
pub struct WrappedHfsPlusInfo {
    /// Byte offset of the wrapper inside the source disk image.
    pub partition_offset: u64,
    /// Size in bytes of the source wrapper partition extent.
    pub source_partition_size: u64,
    /// `drAlBlkSiz` — outer HFS allocation block size (bytes). Always a
    /// multiple of 512, but unlike HFS+ this need not be a power of two.
    pub al_block_size: u32,
    /// `drAlBlSt` — sector index (512-byte units, relative to partition
    /// start) where the wrapper's allocation block area begins.
    pub al_block_start_sector: u16,
    /// `drNmAlBlks` — number of allocation blocks in the source wrapper.
    pub source_total_blocks: u16,
    /// `drVBMSt` — sector index (relative to partition start) of the
    /// volume bitmap.
    pub bitmap_start_sector: u16,
    /// `drEmbedSigWord` — `0x482B` (H+) or `0x4858` (HX).
    pub embed_sig: u16,
    /// `drEmbedExtent.startBlock` — first wrapper allocation block of the
    /// embedded HFS+ volume.
    pub embed_start_block: u16,
    /// `drEmbedExtent.blockCount` — number of wrapper allocation blocks
    /// occupied by the embedded HFS+ volume.
    pub embed_block_count: u16,
    /// Byte offset of the embedded HFS+ inside the source disk image.
    pub inner_offset: u64,
    /// Size in bytes of the embedded HFS+ volume in the source.
    pub inner_size: u64,
}

/// Probe the wrapper MDB at `partition_offset + 1024` and return the
/// parsed wrapper info iff it's a classic HFS volume with an embedded
/// HFS+/HFSX. Returns `None` for any other layout (flat HFS+, native HFS,
/// non-HFS, parse errors).
pub fn detect_wrapped_hfsplus<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    partition_size: u64,
) -> Option<WrappedHfsPlusInfo> {
    reader.seek(SeekFrom::Start(partition_offset + 1024)).ok()?;
    let mut mdb = [0u8; 512];
    reader.read_exact(&mut mdb).ok()?;
    let sig = BigEndian::read_u16(&mdb[0..2]);
    if sig != 0x4244 {
        return None;
    }
    let embed_sig = BigEndian::read_u16(&mdb[124..126]);
    if embed_sig != 0x482B && embed_sig != 0x4858 {
        return None;
    }
    let al_block_size = BigEndian::read_u32(&mdb[20..24]);
    let bitmap_start_sector = BigEndian::read_u16(&mdb[14..16]);
    let total_blocks = BigEndian::read_u16(&mdb[18..20]);
    let al_block_start_sector = BigEndian::read_u16(&mdb[28..30]);
    let embed_start_block = BigEndian::read_u16(&mdb[126..128]);
    let embed_block_count = BigEndian::read_u16(&mdb[128..130]);

    if al_block_size == 0 || al_block_size % 512 != 0 {
        return None;
    }
    if al_block_start_sector == 0 || bitmap_start_sector == 0 {
        return None;
    }
    if embed_block_count == 0 {
        return None;
    }

    let inner_offset = partition_offset
        + (al_block_start_sector as u64) * 512
        + (embed_start_block as u64) * (al_block_size as u64);
    let inner_size = (embed_block_count as u64) * (al_block_size as u64);

    // Sanity: inner extent must fit inside the partition. Use saturating
    // arithmetic so a caller passing `u64::MAX` for the partition size (when
    // the real bound isn't known yet — e.g. the GUI's pre-flight) doesn't
    // overflow.
    let part_end = partition_offset.saturating_add(partition_size);
    let inner_end = inner_offset.saturating_add(inner_size);
    if inner_end > part_end {
        return None;
    }

    Some(WrappedHfsPlusInfo {
        partition_offset,
        source_partition_size: partition_size,
        al_block_size,
        al_block_start_sector,
        source_total_blocks: total_blocks,
        bitmap_start_sector,
        embed_sig,
        embed_start_block,
        embed_block_count,
        inner_offset,
        inner_size,
    })
}

/// Plan for emitting a shrunken wrapped HFS+ volume. All sizes are bytes
/// unless noted.
#[derive(Debug, Clone)]
pub struct WrappedClonePlan {
    pub info: WrappedHfsPlusInfo,
    /// Inner HFS+ size after clone, in bytes. Multiple of both the inner
    /// HFS+ block size and the outer `al_block_size`.
    pub new_inner_size: u64,
    /// Inner extent measured in outer wrapper allocation blocks.
    pub new_embed_block_count: u16,
    /// New `drNmAlBlks` (= `embed_start_block + new_embed_block_count`).
    pub new_total_blocks: u16,
    /// New partition extent size in bytes (multiple of 512).
    pub new_partition_size: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum WrappedCloneError {
    #[error("inner HFS+ block size {0} is not a divisor of the wrapper allocation block size")]
    InnerOuterBlockMismatch(u32),
    #[error("requested inner target {requested} exceeds source extent {available}")]
    TargetExceedsSource { requested: u64, available: u64 },
    #[error("new wrapper allocation block count {count} exceeds the 16-bit HFS field limit")]
    BlockCountOverflow { count: u64 },
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("filesystem: {0}")]
    Filesystem(#[from] FilesystemError),
}

/// Build a [`WrappedClonePlan`] sized for a shrunken inner HFS+ volume.
///
/// `target_inner_size_hint` is the caller's *desired* inner size (typically
/// the inner volume's `defragmented_minimum_size()`). It will be rounded
/// up to the next outer `al_block_size` multiple, and validated against
/// the source's embedded extent.
pub fn plan_wrapped_clone(
    info: &WrappedHfsPlusInfo,
    target_inner_size_hint: u64,
) -> Result<WrappedClonePlan, WrappedCloneError> {
    let outer_bs = info.al_block_size as u64;
    // Round up to outer allocation block boundary. The wrapper packs the
    // inner extent in units of `al_block_size`, so anything finer would
    // break the embedded-extent invariants.
    let new_inner_size = target_inner_size_hint.div_ceil(outer_bs) * outer_bs;
    if new_inner_size > info.inner_size {
        return Err(WrappedCloneError::TargetExceedsSource {
            requested: new_inner_size,
            available: info.inner_size,
        });
    }
    let new_count_u64 = new_inner_size / outer_bs;
    let new_embed_block_count: u16 =
        new_count_u64
            .try_into()
            .map_err(|_| WrappedCloneError::BlockCountOverflow {
                count: new_count_u64,
            })?;
    let new_total_blocks_u32 = info.embed_start_block as u32 + new_embed_block_count as u32;
    let new_total_blocks: u16 =
        new_total_blocks_u32
            .try_into()
            .map_err(|_| WrappedCloneError::BlockCountOverflow {
                count: new_total_blocks_u32 as u64,
            })?;
    // Wrapper layout: alloc-block region + 1024 trailing bytes (alt MDB +
    // reserved last sector). Per IM:Files alt MDB is at sector -2 from
    // partition end, last sector reserved.
    let new_partition_size =
        (info.al_block_start_sector as u64) * 512 + (new_total_blocks as u64) * outer_bs + 1024;

    Ok(WrappedClonePlan {
        info: info.clone(),
        new_inner_size,
        new_embed_block_count,
        new_total_blocks,
        new_partition_size,
    })
}

/// Stream a shrunken wrapped HFS+ volume to `dst`. Single-pass; no seeks
/// required on the writer.
///
/// `reader` must be a `Read + Seek` view of the *source disk* (not the
/// inner volume): wrapper bytes are pulled from the source partition extent
/// at byte offsets computed from `plan.info.partition_offset` etc.
pub fn stream_wrapped_defragmented_hfsplus<R, W>(
    reader: &mut R,
    plan: &WrappedClonePlan,
    dst: &mut W,
) -> Result<DefragReport, WrappedCloneError>
where
    R: Read + Seek + Send,
    W: Write,
{
    let info = &plan.info;
    let mut written: u64 = 0;
    let outer_bs = info.al_block_size as u64;

    // ---------- Boot blocks: bytes [0, 1024) — verbatim from source ----------
    copy_from_source(reader, info.partition_offset, dst, 1024)?;
    written += 1024;

    // ---------- Primary MDB at sector 2 (bytes 1024..1536) ----------
    let mut mdb_buf = [0u8; 512];
    {
        reader.seek(SeekFrom::Start(info.partition_offset + 1024))?;
        reader.read_exact(&mut mdb_buf)?;
    }
    patch_mdb_in_place(&mut mdb_buf, plan);
    dst.write_all(&mdb_buf)?;
    written += 512;

    // ---------- Zero/source bytes from MDB end up to drVBMSt sector ----------
    let bitmap_start = info.bitmap_start_sector as u64 * 512;
    if bitmap_start < 1536 {
        // Pathological wrapper — drVBMSt must be ≥ 3 per HFS spec.
        return Err(WrappedCloneError::Filesystem(FilesystemError::InvalidData(
            format!("wrapper drVBMSt {} too low", info.bitmap_start_sector),
        )));
    }
    if bitmap_start > 1536 {
        copy_from_source(
            reader,
            info.partition_offset + 1536,
            dst,
            bitmap_start - 1536,
        )?;
        written += bitmap_start - 1536;
    }

    // ---------- Volume bitmap [drVBMSt, drAlBlSt) — source bytes, last bits cleared ----------
    let alloc_start = info.al_block_start_sector as u64 * 512;
    if alloc_start <= bitmap_start {
        return Err(WrappedCloneError::Filesystem(FilesystemError::InvalidData(
            format!(
                "wrapper drAlBlSt {} not greater than drVBMSt {}",
                info.al_block_start_sector, info.bitmap_start_sector,
            ),
        )));
    }
    let bitmap_len = alloc_start - bitmap_start;
    let mut bitmap = vec![0u8; bitmap_len as usize];
    {
        reader.seek(SeekFrom::Start(info.partition_offset + bitmap_start))?;
        reader.read_exact(&mut bitmap)?;
    }
    clear_bitmap_tail(&mut bitmap, plan.new_total_blocks);
    dst.write_all(&bitmap)?;
    written += bitmap_len;

    // ---------- Wrapper alloc blocks [0, embed_start) — verbatim ----------
    let pre_embed_bytes = (info.embed_start_block as u64) * outer_bs;
    if pre_embed_bytes > 0 {
        copy_from_source(
            reader,
            info.partition_offset + alloc_start,
            dst,
            pre_embed_bytes,
        )?;
        written += pre_embed_bytes;
    }

    // ---------- Embedded HFS+ — clone via flat-HFS+ streamer ----------
    //
    // Reopen the inner volume as its own HfsPlusFilesystem at the source's
    // inner_offset. The clone targets `plan.new_inner_size`, which we
    // already validated as a multiple of the outer alloc-block size (≥
    // the inner block size in every real-world wrapper).
    let inner_clone_reader = clone_reader(reader)?;
    let mut inner_fs =
        HfsPlusFilesystem::open(BufReader::new(inner_clone_reader), info.inner_offset)
            .map_err(WrappedCloneError::Filesystem)?;

    // The planner aligns `new_inner_size` to the outer `al_block_size`,
    // which is normally a clean multiple of the inner HFS+ block size
    // (4 KiB). Validate it explicitly so a corrupt source surfaces here
    // instead of mid-stream.
    let inner_bs = inner_fs.block_size();
    if plan.new_inner_size % inner_bs as u64 != 0 {
        return Err(WrappedCloneError::InnerOuterBlockMismatch(inner_bs));
    }

    let report = stream_defragmented_hfsplus(&mut inner_fs, plan.new_inner_size, dst)
        .map_err(WrappedCloneError::Filesystem)?;
    written += plan.new_inner_size;

    // ---------- Tail: zero pad to (new_partition_size - 1024) ----------
    let alt_mdb_offset = plan.new_partition_size - 1024;
    if written > alt_mdb_offset {
        return Err(WrappedCloneError::Filesystem(FilesystemError::InvalidData(
            format!(
                "wrapper clone over-emitted: wrote {written} bytes but alt MDB sits at {alt_mdb_offset}"
            ),
        )));
    }
    write_zeros(dst, alt_mdb_offset - written)?;
    written = alt_mdb_offset;

    // ---------- Alternate MDB (sector -2 from partition end) ----------
    dst.write_all(&mdb_buf)?;
    written += 512;

    // ---------- Reserved last sector ----------
    write_zeros(dst, 512)?;
    written += 512;

    debug_assert_eq!(written, plan.new_partition_size);
    Ok(report)
}

fn patch_mdb_in_place(mdb: &mut [u8; 512], plan: &WrappedClonePlan) {
    // drNmAlBlks @ 18 (u16 BE).
    BigEndian::write_u16(&mut mdb[18..20], plan.new_total_blocks);
    // drFreeBks @ 34 (u16 BE). The wrapper is fully allocated: its own
    // bookkeeping files fill `[0..embed_start)` and the embedded extent
    // fills `[embed_start..new_total_blocks)`. Nothing free.
    BigEndian::write_u16(&mut mdb[34..36], 0);
    // drEmbedExtent.blockCount @ 128 (u16 BE).
    BigEndian::write_u16(&mut mdb[128..130], plan.new_embed_block_count);
    // drEmbedExtent.startBlock @ 126 stays unchanged so the inner
    // landing offset within the wrapper doesn't move.
}

fn clear_bitmap_tail(bitmap: &mut [u8], new_total_blocks: u16) {
    let n = new_total_blocks as usize;
    let byte_idx = n / 8;
    let bit_idx = n % 8;
    if byte_idx >= bitmap.len() {
        return;
    }
    // Within the byte that straddles the boundary: keep the low `bit_idx`
    // MSB-counted bits (HFS bitmap is big-endian: bit 0 = MSB of byte 0
    // covers alloc block 0).
    if bit_idx > 0 {
        // Mask keeping the top `bit_idx` bits.
        let keep_mask: u8 = !((1u8 << (8 - bit_idx)) - 1);
        bitmap[byte_idx] &= keep_mask;
    } else {
        bitmap[byte_idx] = 0;
    }
    for byte in bitmap[byte_idx + 1..].iter_mut() {
        *byte = 0;
    }
}

const ZEROS: &[u8] = &[0u8; 65536];

fn write_zeros<W: Write>(dst: &mut W, mut remaining: u64) -> io::Result<()> {
    while remaining > 0 {
        let n = remaining.min(ZEROS.len() as u64) as usize;
        dst.write_all(&ZEROS[..n])?;
        remaining -= n as u64;
    }
    Ok(())
}

fn copy_from_source<R: Read + Seek, W: Write>(
    reader: &mut R,
    src_offset: u64,
    dst: &mut W,
    len: u64,
) -> io::Result<()> {
    reader.seek(SeekFrom::Start(src_offset))?;
    let mut buf = [0u8; 65536];
    let mut remaining = len;
    while remaining > 0 {
        let n = remaining.min(buf.len() as u64) as usize;
        reader.read_exact(&mut buf[..n])?;
        dst.write_all(&buf[..n])?;
        remaining -= n as u64;
    }
    Ok(())
}

/// Build a fresh `Read + Seek` view of the same backing source so the
/// inner `HfsPlusFilesystem` can be opened without interfering with the
/// outer reader's cursor. Falls back to a wrapped clone via `try_clone`
/// for `File`-backed sources; in-memory test sources implement a
/// no-op clone trick documented in the test helpers.
fn clone_reader<R: Read + Seek>(reader: &mut R) -> Result<ReaderClone<'_, R>, WrappedCloneError> {
    // The trait-object reader can't be Cloned generically. We need a
    // separate Read+Seek over the same byte stream. The simplest portable
    // path: dump nothing — just return a positional wrapper that re-uses
    // `reader` via a shared cursor. Since `stream_wrapped_defragmented_hfsplus`
    // is single-pass on `dst` and the inner clone consumes the reader
    // strictly via Seek+Read (no concurrent use), interleaving is fine.
    Ok(ReaderClone {
        // Safety: we move the &mut R into a wrapper that's only used for
        // the duration of the inner clone. The outer streamer doesn't
        // re-issue reads on `reader` until the inner clone returns.
        inner: reader,
    })
}

/// Adapter that re-borrows the source reader for the inner HFS+ open.
struct ReaderClone<'a, R> {
    inner: &'a mut R,
}

impl<'a, R: Read> Read for ReaderClone<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<'a, R: Seek> Seek for ReaderClone<'a, R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    use crate::fs::hfsplus::create_blank_hfsplus;

    /// Build a minimal wrapped HFS+ image:
    ///   - boot blocks (1024 bytes, zeros)
    ///   - primary MDB at sector 2 with drEmbedSigWord/drEmbedExtent set
    ///   - drVBMSt = 3, bitmap covers `drNmAlBlks` bits then zero pad
    ///   - drAlBlSt = `bitmap_sectors + 3`
    ///   - Wrapper alloc block 0 = zeros (no real wrapper B-trees — the
    ///     clone path treats this region opaquely as "wrapper bookkeeping")
    ///   - Embedded HFS+ at alloc block `embed_start`, sized to fit
    ///     `inner_image_size_bytes`
    ///   - Alt MDB at partition_size - 1024
    fn build_wrapped_test_image(
        inner_block_size: u32,
        inner_size: u64,
        outer_al_block_size: u32,
        embed_start_block: u16,
    ) -> (Vec<u8>, /*partition_size*/ u64) {
        assert_eq!(inner_size % outer_al_block_size as u64, 0);
        let inner = create_blank_hfsplus(inner_size, inner_block_size, "Inner", false);
        assert_eq!(inner.len() as u64, inner_size);

        let outer_bs = outer_al_block_size as u64;
        let embed_block_count = (inner_size / outer_bs) as u16;
        let total_blocks: u16 = embed_start_block + embed_block_count;
        let bitmap_bits = total_blocks as u64;
        let bitmap_bytes = bitmap_bits.div_ceil(8);
        let bitmap_sectors = bitmap_bytes.div_ceil(512) as u16;
        let drvbmst: u16 = 3;
        let dralblst: u16 = drvbmst + bitmap_sectors;

        let alloc_start_bytes = (dralblst as u64) * 512;
        let inner_offset = alloc_start_bytes + (embed_start_block as u64) * outer_bs;
        let partition_size = alloc_start_bytes + (total_blocks as u64) * outer_bs + 1024;

        let mut img = vec![0u8; partition_size as usize];

        // MDB at sector 2.
        let mdb_off = 1024;
        let mdb = &mut img[mdb_off..mdb_off + 512];
        BigEndian::write_u16(&mut mdb[0..2], 0x4244);
        BigEndian::write_u16(&mut mdb[14..16], drvbmst);
        BigEndian::write_u16(&mut mdb[18..20], total_blocks); // drNmAlBlks
        BigEndian::write_u32(&mut mdb[20..24], outer_al_block_size); // drAlBlkSiz
        BigEndian::write_u16(&mut mdb[28..30], dralblst); // drAlBlSt
        BigEndian::write_u16(&mut mdb[34..36], 0); // drFreeBks
        BigEndian::write_u16(&mut mdb[124..126], 0x482B); // drEmbedSigWord
        BigEndian::write_u16(&mut mdb[126..128], embed_start_block);
        BigEndian::write_u16(&mut mdb[128..130], embed_block_count);

        // Bitmap: first `total_blocks` bits set.
        let bitmap_off = (drvbmst as usize) * 512;
        for bit in 0..(total_blocks as usize) {
            img[bitmap_off + bit / 8] |= 0x80 >> (bit % 8);
        }

        // Inner HFS+ payload at the embedded extent.
        img[inner_offset as usize..(inner_offset + inner_size) as usize].copy_from_slice(&inner);

        // Alt MDB mirror at -1024.
        let alt_off = partition_size as usize - 1024;
        let mdb_copy = img[mdb_off..mdb_off + 512].to_vec();
        img[alt_off..alt_off + 512].copy_from_slice(&mdb_copy);

        (img, partition_size)
    }

    #[test]
    fn detect_recognises_synthesised_wrapper() {
        let (img, partition_size) = build_wrapped_test_image(4096, 4 * 1024 * 1024, 4096, 1);
        let mut cur = Cursor::new(&img);
        let info =
            detect_wrapped_hfsplus(&mut cur, 0, partition_size).expect("should detect wrapper");
        assert_eq!(info.embed_sig, 0x482B);
        assert_eq!(info.al_block_size, 4096);
        assert_eq!(info.embed_start_block, 1);
        assert_eq!(info.embed_block_count as u32, (4 * 1024 * 1024) / 4096);
        assert_eq!(info.inner_size, 4 * 1024 * 1024);
    }

    #[test]
    fn round_trip_clones_at_source_size() {
        // 512 KiB inner, 4 KiB outer alloc blocks.
        let (img, partition_size) = build_wrapped_test_image(4096, 4 * 1024 * 1024, 4096, 1);
        let mut cur = Cursor::new(img.clone());
        let info = detect_wrapped_hfsplus(&mut cur, 0, partition_size).unwrap();
        // Clone at the same inner size — no shrink, just round-trip.
        let plan = plan_wrapped_clone(&info, info.inner_size).unwrap();
        assert_eq!(plan.new_inner_size, info.inner_size);
        assert_eq!(plan.new_partition_size, partition_size);

        let mut out: Vec<u8> = Vec::with_capacity(partition_size as usize);
        let mut src_cur = Cursor::new(img.as_slice());
        let report = stream_wrapped_defragmented_hfsplus(&mut src_cur, &plan, &mut out)
            .expect("clone should succeed");
        // Empty inner volume — no files copied, but stream completed.
        assert_eq!(report.files_copied, 0);
        assert_eq!(out.len(), partition_size as usize);

        // Verify wrapper survived: signature + embed pointer intact.
        let mut out_cur = Cursor::new(out.as_slice());
        let info2 = detect_wrapped_hfsplus(&mut out_cur, 0, partition_size)
            .expect("output must still parse as wrapper");
        assert_eq!(info2.embed_sig, info.embed_sig);
        assert_eq!(info2.embed_start_block, info.embed_start_block);
        assert_eq!(info2.embed_block_count, info.embed_block_count);

        // Inner HFS+ must be openable at the resolved offset.
        let mut inner_cur = Cursor::new(out.as_slice());
        let _hfs = crate::fs::hfsplus::HfsPlusFilesystem::open(&mut inner_cur, info2.inner_offset)
            .expect("inner HFS+ must be valid after wrapper round-trip");
    }

    #[test]
    fn shrink_keeps_wrapper_and_inner_valid() {
        // Build an 8 MiB inner; clone targeting 1 MiB. The blank HFS+
        // body fits well under 1 MiB, so the planner should accept it.
        let (img, partition_size) = build_wrapped_test_image(4096, 8 * 1024 * 1024, 4096, 1);
        let mut cur = Cursor::new(img.clone());
        let info = detect_wrapped_hfsplus(&mut cur, 0, partition_size).unwrap();

        let target_inner = 1024 * 1024u64;
        let plan = plan_wrapped_clone(&info, target_inner).unwrap();
        assert_eq!(plan.new_inner_size, target_inner);
        assert!(plan.new_partition_size < partition_size);

        let mut out: Vec<u8> = Vec::with_capacity(plan.new_partition_size as usize);
        let mut src_cur = Cursor::new(img.as_slice());
        stream_wrapped_defragmented_hfsplus(&mut src_cur, &plan, &mut out)
            .expect("shrink clone should succeed");
        assert_eq!(out.len(), plan.new_partition_size as usize);

        // Output: wrapper still parses and inner HFS+ opens at resolved offset.
        let mut out_cur = Cursor::new(out.as_slice());
        let info2 = detect_wrapped_hfsplus(&mut out_cur, 0, plan.new_partition_size).unwrap();
        assert_eq!(
            info2.embed_block_count, plan.new_embed_block_count,
            "wrapper must reflect the new embedded extent size"
        );
        let mut inner_cur = Cursor::new(out.as_slice());
        let _hfs = crate::fs::hfsplus::HfsPlusFilesystem::open(&mut inner_cur, info2.inner_offset)
            .expect("inner HFS+ must be valid after wrapper shrink");
    }

    #[test]
    fn clear_bitmap_tail_zeros_past_boundary() {
        // 24 blocks worth of bitmap, all 1s.
        let mut bm = vec![0xFFu8; 4];
        clear_bitmap_tail(&mut bm, 10);
        // Blocks 0..10 set, 10..32 cleared. Bit 0 = MSB.
        assert_eq!(bm[0], 0xFF);
        assert_eq!(bm[1], 0b1100_0000);
        assert_eq!(bm[2], 0x00);
        assert_eq!(bm[3], 0x00);
    }

    #[test]
    fn clear_bitmap_tail_byte_boundary() {
        let mut bm = vec![0xFFu8; 4];
        clear_bitmap_tail(&mut bm, 16);
        assert_eq!(bm[0], 0xFF);
        assert_eq!(bm[1], 0xFF);
        assert_eq!(bm[2], 0x00);
        assert_eq!(bm[3], 0x00);
    }

    #[test]
    fn plan_rounds_inner_to_outer_block() {
        let info = WrappedHfsPlusInfo {
            partition_offset: 0,
            source_partition_size: 1024 * 1024 * 1024, // 1 GiB
            al_block_size: 32 * 1024,                  // 32 KiB outer blocks
            al_block_start_sector: 19,
            source_total_blocks: 32000,
            bitmap_start_sector: 3,
            embed_sig: 0x482B,
            embed_start_block: 8,
            embed_block_count: 30000,
            inner_offset: 19 * 512 + 8 * 32 * 1024,
            inner_size: 30000u64 * 32 * 1024,
        };
        // Ask for ~5 MiB; expect rounding up to a multiple of 32 KiB.
        let hint = 5 * 1024 * 1024 + 17; // not aligned
        let plan = plan_wrapped_clone(&info, hint).unwrap();
        assert_eq!(plan.new_inner_size % (32 * 1024), 0);
        assert!(plan.new_inner_size >= hint);
        // new_total_blocks = embed_start + new_count
        assert_eq!(
            plan.new_total_blocks as u64,
            info.embed_start_block as u64 + plan.new_embed_block_count as u64
        );
        // partition_size = alloc_start*512 + new_total*outer_bs + 1024
        let expected_size = 19u64 * 512 + (plan.new_total_blocks as u64) * 32 * 1024 + 1024;
        assert_eq!(plan.new_partition_size, expected_size);
    }
}
