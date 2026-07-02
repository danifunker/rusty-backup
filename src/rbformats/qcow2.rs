//! QCOW2 reader (versions 2 and 3, uncompressed, single-file).
//!
//! Phase 2 of `docs/virtualization-formats.md` — Session 2.1 ships **read-only**
//! support so inspect / browse / convert-from work against QCOW2 images
//! (notably UTM's classic-Mac PPC disks). Writer + in-place edit land in
//! sessions 2.2 — 2.4.
//!
//! ## Scope
//!
//! Accept **header versions 2 and 3** (`compat=0.10` and `compat=1.1`). Reject
//! qcow1 and anything beyond v3. The reader emits zeros for unallocated clusters
//! and for v3's QCOW_OFLAG_ZERO (L2 bit 0 on a *standard* entry). Compressed
//! clusters (L2 bit 62), backing files, encryption, snapshots, external data
//! files, extended L2 entries, non-default refcount widths, and any unknown
//! `incompatible_features` bit all error out at open time with a precise
//! message — Session 2.1 is "open it, verify it's plain, present it as a flat
//! `Read + Seek` byte stream".
//!
//! ## Layout (all big-endian)
//!
//! ```text
//! +0      Header (v2: 72 B, v3: ≥ 104 B + padding)
//! ...     L1 table: l1_size * u64
//! ...     L2 tables (one per non-empty L1 slot): cluster_size / 8 * u64
//! ...     Refcount table → refcount blocks (ignored on read)
//! ...     Data clusters
//! ```
//!
//! Address translation (read path) for a guest byte offset `off`:
//!
//! ```text
//! cluster      = off / cluster_size
//! l2_entries   = cluster_size / 8
//! l1_index     = cluster / l2_entries
//! l2_index     = cluster % l2_entries
//! intra        = off % cluster_size
//! ```
//!
//! `L1[l1_index]`: bit 63 COPIED, bits `0x00FFFFFFFFFFFE00` = host offset of the
//! L2 table; entry `0` means "no L2 yet" → zeros. `L2[l2_index]`: bit 63 COPIED,
//! bit 62 compressed (rejected), bit 0 (v3 only) QCOW_OFLAG_ZERO, low bits =
//! host cluster offset; `0` means unallocated → zeros.

use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::{bail, Context, Result};

/// QCOW2 magic at file offset 0 (`"QFI\xFB"`).
pub const QCOW2_MAGIC: &[u8; 4] = b"QFI\xfb";

/// Mask for the host-offset bits of an L1 or standard L2 entry (bits 9..55).
const QCOW2_OFFSET_MASK: u64 = 0x00FF_FFFF_FFFF_FE00;
/// L1/L2 entry bit 63 — refcount is exactly 1 (single owner). We treat this as
/// informational and never validate it for v1 scope.
#[allow(dead_code)] // writer (Session 2.2) sets this on every emitted L1/L2 entry.
const QCOW2_FLAG_COPIED: u64 = 1u64 << 63;
/// L2 entry bit 62 — cluster is compressed. Rejected in v1 scope.
const QCOW2_FLAG_COMPRESSED: u64 = 1u64 << 62;
/// Default `cluster_bits` for the writer: 16 → 64 KiB clusters (the qemu-img
/// default since v1.0, and what every retro VM image in the wild uses).
pub const QCOW2_DEFAULT_CLUSTER_BITS: u32 = 16;
/// Header length the writer stamps in v3 images (104 = spec minimum; we emit no
/// header extensions). We still allocate 112 bytes on disk so the 8-byte
/// `compression_type` + padding field is aligned to a 16-byte boundary,
/// matching what `qemu-img` produces.
const QCOW2_V3_HEADER_LEN: u32 = 104;
/// L2 entry bit 0 (v3 only) on a *standard* entry — "read as zeros". The same
/// bit position is reserved on v2, where unallocated zero clusters come from
/// `entry == 0` instead.
const QCOW2_FLAG_ZERO: u64 = 1;

/// Defined v3 `incompatible_features` bits. Any bit outside this mask is
/// "unknown" and aborts open per the spec ("if any unknown bit is set the image
/// must not be opened").
///
/// - bit 0: dirty
/// - bit 1: corrupt
/// - bit 2: external data file (rejected — would need a second file)
/// - bit 3: compression type (implies a non-zlib codec — rejected)
/// - bit 4: extended L2 entries (subclusters — rejected, layout differs)
const QCOW2_KNOWN_INCOMPAT: u64 = 0b1_1111;

/// Parsed QCOW2 header, normalized so v2 and v3 callers go through the same
/// fields. v2 entries that don't exist on disk default to zero — that matches
/// the spec's "treat missing v3 fields as zero on v2" interpretation.
#[derive(Debug, Clone, Copy)]
struct Qcow2Header {
    version: u32,
    cluster_size: u64,
    size: u64,
}

/// Reader over a v2/v3 QCOW2 image presenting the virtual disk as a flat
/// `Read + Seek` byte stream. Unallocated and `QCOW_OFLAG_ZERO` clusters read
/// as zeros; one inner read never crosses a cluster boundary.
pub struct Qcow2Reader<R: Read + Seek> {
    inner: R,
    header: Qcow2Header,
    /// L1 table, fully loaded into RAM (small: at 64 KiB clusters,
    /// `l1_size = ceil(disk / (cluster_size * cluster_size / 8))` — a 2 TiB disk
    /// gives 512 entries = 4 KiB).
    l1: Vec<u64>,
    /// Cached L2 table for the most recent host offset (single-entry LRU, same
    /// shape `ChdReader` uses for its hunk cache).
    l2_cache: Option<(u64, Vec<u64>)>,
    /// Current logical (guest-side) byte position.
    pos: u64,
    /// Host byte offset of the L1 table — needed by the in-place editor to
    /// patch one L1 entry when it grows a new L2 table.
    l1_table_offset: u64,
    /// Refcount table (one u64 entry per refcount block, parsed from disk).
    /// Session 2.4 grows this on demand by appending fresh refcount blocks at
    /// EOF and patching the on-disk table entry.
    refcount_table: Vec<u64>,
    /// Host byte offset of the refcount table — needed to patch one entry on
    /// disk when [`Self::set_refcount_one`] allocates a new refcount block.
    refcount_table_offset: u64,
    /// Number of u64 slots the refcount table itself can hold
    /// (`refcount_table_clusters * cluster_size / 8`). Growing the *table* (as
    /// opposed to its blocks) would require relocating the entire structure
    /// and is refused with a clear error — `qemu-img check`-clean images at
    /// realistic sizes never need it (one cluster of table addresses
    /// `cluster_size/8` blocks ≈ 16 TiB at 64 KiB clusters).
    refcount_table_capacity: usize,
    /// Free host clusters available for reuse before falling back to
    /// `host_end` append. Lazily populated on the first allocation: scan the
    /// refcount blocks once and remember every cluster slot whose refcount is
    /// zero but whose host offset is below `host_end`. Subsequent edits drain
    /// this Vec first.
    free_clusters: Option<Vec<u64>>,
    /// Current end-of-file in bytes — where the next allocate-on-write cluster
    /// lands when `free_clusters` is empty. Initialised to the file length at
    /// open time; advanced by `cluster_size` per host-end alloc.
    host_end: u64,
}

impl<R: Read + Seek> Qcow2Reader<R> {
    /// Parse the QCOW2 header + L1 table. Errors describe exactly which feature
    /// was rejected (encryption, snapshots, etc.) so the user-facing message is
    /// actionable.
    pub fn open(mut inner: R) -> Result<Self> {
        let total_len = inner.seek(SeekFrom::End(0))?;
        if total_len < 72 {
            bail!("file too small to be a QCOW2 image");
        }

        inner.seek(SeekFrom::Start(0))?;
        let mut head = [0u8; 104];
        let head_to_read = head.len().min(total_len as usize);
        inner.read_exact(&mut head[..head_to_read])?;

        if &head[0..4] != QCOW2_MAGIC {
            bail!("missing QCOW2 magic");
        }
        let version = u32::from_be_bytes(head[4..8].try_into().unwrap());
        match version {
            2 | 3 => {}
            1 => bail!("qcow1 images are not supported (use qemu-img convert -O qcow2)"),
            other => bail!("unsupported QCOW2 version {other}"),
        }

        let backing_file_offset = u64::from_be_bytes(head[8..16].try_into().unwrap());
        if backing_file_offset != 0 {
            bail!("QCOW2 with a backing file is not supported");
        }

        let cluster_bits = u32::from_be_bytes(head[20..24].try_into().unwrap());
        if !(9..=21).contains(&cluster_bits) {
            // qemu enforces 9..=21 (512 B .. 2 MiB); narrower than spec but
            // wider than we'll ever see in the wild.
            bail!("QCOW2 cluster_bits {cluster_bits} out of supported range 9..=21");
        }
        let cluster_size = 1u64 << cluster_bits;
        let size = u64::from_be_bytes(head[24..32].try_into().unwrap());
        let crypt_method = u32::from_be_bytes(head[32..36].try_into().unwrap());
        if crypt_method != 0 {
            bail!("encrypted QCOW2 images are not supported");
        }
        let l1_size = u32::from_be_bytes(head[36..40].try_into().unwrap());
        let l1_table_offset = u64::from_be_bytes(head[40..48].try_into().unwrap());
        let nb_snapshots = u32::from_be_bytes(head[60..64].try_into().unwrap());
        if nb_snapshots != 0 {
            bail!("QCOW2 with snapshots is not supported");
        }

        if version == 3 {
            if total_len < 104 {
                bail!("v3 QCOW2 header truncated");
            }
            let incompat = u64::from_be_bytes(head[72..80].try_into().unwrap());
            if incompat & !QCOW2_KNOWN_INCOMPAT != 0 {
                bail!(
                    "QCOW2 image sets unknown incompatible_features bits ({incompat:#018x}); \
                     refusing to open"
                );
            }
            if incompat & 0b0_0100 != 0 {
                bail!("QCOW2 with an external data file is not supported");
            }
            if incompat & 0b0_1000 != 0 {
                bail!("QCOW2 with a non-default compression type is not supported");
            }
            if incompat & 0b1_0000 != 0 {
                bail!("QCOW2 with extended L2 entries (subclusters) is not supported");
            }
            let refcount_order = u32::from_be_bytes(head[96..100].try_into().unwrap());
            if refcount_order != 4 {
                bail!("QCOW2 refcount_order {refcount_order} not supported (need 4)");
            }
            let header_length = u32::from_be_bytes(head[100..104].try_into().unwrap()) as u64;
            if header_length < 104 || header_length > total_len {
                bail!("QCOW2 v3 header_length {header_length} out of range");
            }
        }

        // L1 table size sanity: must fit in the file and L1 entries per cluster
        // address everything the virtual size demands.
        let entries_per_l2 = cluster_size / 8;
        let required_l1 = size.div_ceil(cluster_size * entries_per_l2);
        if (l1_size as u64) < required_l1 {
            bail!(
                "QCOW2 l1_size {l1_size} too small for disk size {size} \
                 (need at least {required_l1})"
            );
        }
        if l1_table_offset == 0 || l1_table_offset + (l1_size as u64) * 8 > total_len {
            bail!("QCOW2 L1 table extends past end of file");
        }

        inner.seek(SeekFrom::Start(l1_table_offset))?;
        let mut l1_bytes = vec![0u8; l1_size as usize * 8];
        inner.read_exact(&mut l1_bytes)?;
        let l1: Vec<u64> = l1_bytes
            .chunks_exact(8)
            .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
            .collect();

        // Refcount table — parsed even for read-only opens so the editor doesn't
        // need a separate re-parse later. Cheap: one cluster of u64 entries for
        // typical images.
        let refcount_table_offset = u64::from_be_bytes(head[48..56].try_into().unwrap());
        let refcount_table_clusters = u32::from_be_bytes(head[56..60].try_into().unwrap()) as u64;
        let (refcount_table, refcount_table_capacity) =
            if refcount_table_offset == 0 || refcount_table_clusters == 0 {
                // Strict open would refuse this — the spec mandates a refcount
                // table on v3. Be permissive on read (returns an empty Vec, so
                // the editor refuses to allocate) so we still inspect odd
                // inputs.
                (Vec::new(), 0usize)
            } else {
                let rt_bytes = refcount_table_clusters * cluster_size;
                if refcount_table_offset + rt_bytes > total_len {
                    bail!("QCOW2 refcount table extends past end of file");
                }
                inner.seek(SeekFrom::Start(refcount_table_offset))?;
                let mut buf = vec![0u8; rt_bytes as usize];
                inner.read_exact(&mut buf)?;
                let entries: Vec<u64> = buf
                    .chunks_exact(8)
                    .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
                    .collect();
                let cap = entries.len();
                (entries, cap)
            };

        Ok(Self {
            inner,
            header: Qcow2Header {
                version,
                cluster_size,
                size,
            },
            l1,
            l2_cache: None,
            pos: 0,
            l1_table_offset,
            refcount_table,
            refcount_table_offset,
            refcount_table_capacity,
            free_clusters: None,
            host_end: total_len,
        })
    }

    /// Virtual disk size in bytes (header `size`).
    pub fn len(&self) -> u64 {
        self.header.size
    }

    /// True if the virtual disk is empty.
    pub fn is_empty(&self) -> bool {
        self.header.size == 0
    }

    /// Header version (2 or 3) — exposed for diagnostics + the inspect summary.
    pub fn version(&self) -> u32 {
        self.header.version
    }

    /// Load (and cache) the L2 table at `host_offset`.
    fn load_l2(&mut self, host_offset: u64) -> std::io::Result<&[u64]> {
        if self
            .l2_cache
            .as_ref()
            .map(|(o, _)| *o == host_offset)
            .unwrap_or(false)
        {
            return Ok(&self.l2_cache.as_ref().unwrap().1);
        }
        let entries = (self.header.cluster_size / 8) as usize;
        let mut buf = vec![0u8; entries * 8];
        self.inner.seek(SeekFrom::Start(host_offset))?;
        self.inner.read_exact(&mut buf)?;
        let l2: Vec<u64> = buf
            .chunks_exact(8)
            .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
            .collect();
        self.l2_cache = Some((host_offset, l2));
        Ok(&self.l2_cache.as_ref().unwrap().1)
    }
}

impl<R: Read + Seek> Read for Qcow2Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.header.size || buf.is_empty() {
            return Ok(0);
        }
        let cluster_size = self.header.cluster_size;
        let intra = self.pos % cluster_size;
        let remaining_in_cluster = cluster_size - intra;
        let remaining_in_disk = self.header.size - self.pos;
        let to_read = (buf.len() as u64)
            .min(remaining_in_cluster)
            .min(remaining_in_disk) as usize;

        let cluster_index = self.pos / cluster_size;
        let entries_per_l2 = cluster_size / 8;
        let l1_index = (cluster_index / entries_per_l2) as usize;
        let l2_index = (cluster_index % entries_per_l2) as usize;

        let l1_entry = self.l1.get(l1_index).copied().unwrap_or(0);
        let l2_host = l1_entry & QCOW2_OFFSET_MASK;
        if l2_host == 0 {
            // No L2 → cluster is unallocated → zeros.
            for b in buf[..to_read].iter_mut() {
                *b = 0;
            }
            self.pos += to_read as u64;
            return Ok(to_read);
        }

        let l2 = self.load_l2(l2_host)?;
        let l2_entry = l2[l2_index];

        if l2_entry & QCOW2_FLAG_COMPRESSED != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed QCOW2 clusters are not supported",
            ));
        }
        // QCOW_OFLAG_ZERO is v3-only; on v2 the bit is reserved/unused. We only
        // treat it as "read zeros" for v3 to stay strict on v2.
        let zero_flag = self.header.version == 3 && (l2_entry & QCOW2_FLAG_ZERO) != 0;
        let data_host = l2_entry & QCOW2_OFFSET_MASK;
        if zero_flag || data_host == 0 {
            for b in buf[..to_read].iter_mut() {
                *b = 0;
            }
        } else {
            self.inner.seek(SeekFrom::Start(data_host + intra))?;
            self.inner.read_exact(&mut buf[..to_read])?;
        }
        self.pos += to_read as u64;
        Ok(to_read)
    }
}

impl<R: Read + Seek> Seek for Qcow2Reader<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(n) => self.header.size as i64 + n,
            SeekFrom::Current(n) => self.pos as i64 + n,
        };
        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek before start of QCOW2 image",
            ));
        }
        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

/// Allocate-on-write `Write` for in-place editing of QCOW2 images.
///
/// Writes that land in an already-allocated data cluster update in place.
/// Writes into an unallocated cluster (or a `QCOW_OFLAG_ZERO` cluster on v3)
/// first append a fresh host cluster at the current EOF, patch the owning L2
/// entry, bump the refcount, and — if no L2 table covered that L1 slot yet —
/// also append a fresh L2 cluster and patch the L1 entry.
///
/// **Session 2.3 scope:** refcount **block** updates are in-place, so the
/// covering refcount block must already exist. If the refcount table slot for
/// the new cluster is empty (i.e. refcount-table growth would be needed),
/// `write` errors with `WriteZero` — that growth path is session 2.4.
///
/// Like the reader, one inner `write` never crosses a cluster boundary; the
/// caller (or `write_all`) loops to cover larger writes.
impl<R: Read + Write + Seek> Write for Qcow2Reader<R> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.pos >= self.header.size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write past end of QCOW2 image",
            ));
        }
        let cluster_size = self.header.cluster_size;
        let intra = self.pos % cluster_size;
        let remaining_in_cluster = cluster_size - intra;
        let remaining_in_disk = self.header.size - self.pos;
        let to_write = (buf.len() as u64)
            .min(remaining_in_cluster)
            .min(remaining_in_disk) as usize;

        let cluster_index = self.pos / cluster_size;
        let entries_per_l2 = cluster_size / 8;
        let l1_idx = (cluster_index / entries_per_l2) as usize;
        let l2_idx = (cluster_index % entries_per_l2) as usize;

        // Step 1: ensure an L2 table exists for this L1 slot. Empty slot →
        // allocate a fresh zero-filled cluster, patch L1 on disk + in RAM.
        let l1_entry = self.l1.get(l1_idx).copied().unwrap_or(0);
        let mut l2_host = l1_entry & QCOW2_OFFSET_MASK;
        if l2_host == 0 {
            let new_l2 = self.allocate_cluster(true)?;
            self.l1[l1_idx] = new_l2 | QCOW2_FLAG_COPIED;
            self.write_l1_entry(l1_idx)?;
            l2_host = new_l2;
        }

        // Step 2: locate the L2 entry for this guest cluster. Read straight from
        // disk to avoid juggling the read-side L2 cache through this write path
        // (we invalidate it on any change below).
        let l2_entry = self.read_l2_entry_raw(l2_host, l2_idx)?;
        let mut data_host = l2_entry & QCOW2_OFFSET_MASK;
        let zero_flag = self.header.version == 3 && (l2_entry & QCOW2_FLAG_ZERO) != 0;
        let needs_alloc = data_host == 0 || zero_flag;
        // Compressed clusters: refuse — we never produce them, and overwriting
        // one in place would corrupt the compression. Caller has to re-encode.
        if l2_entry & QCOW2_FLAG_COMPRESSED != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot edit a compressed QCOW2 cluster in place",
            ));
        }
        if needs_alloc {
            let new_data = self.allocate_cluster(true)?;
            let new_entry = new_data | QCOW2_FLAG_COPIED;
            self.write_l2_entry_raw(l2_host, l2_idx, new_entry)?;
            // The freshly-allocated host cluster is zero-filled, so partial
            // writes preserve "unwritten part of this guest cluster reads as 0".
            data_host = new_data;
            // Invalidate L2 cache wholesale if it covered this L2 table — keeps
            // subsequent reads honest without juggling per-entry dirty flags.
            if let Some((cached, _)) = &self.l2_cache {
                if *cached == l2_host {
                    self.l2_cache = None;
                }
            }
        }

        self.inner.seek(SeekFrom::Start(data_host + intra))?;
        self.inner.write_all(&buf[..to_write])?;
        self.pos += to_write as u64;
        Ok(to_write)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<R: Read + Write + Seek> Qcow2Reader<R> {
    /// Acquire a fresh host cluster, optionally zero-filling it, and bump its
    /// refcount. Tries to reuse a free cluster (refcount==0 below `host_end`)
    /// first so opening → editing → closing repeatedly doesn't bloat the file;
    /// falls back to appending a cluster at the current EOF.
    ///
    /// `zero_fill = true` is the right answer for L2 tables and for data
    /// clusters that may only be partially written below — the zero region is
    /// what guarantees "untouched bytes of this guest cluster read as 0".
    fn allocate_cluster(&mut self, zero_fill: bool) -> std::io::Result<u64> {
        self.ensure_free_cluster_index()?;
        let cluster_size = self.header.cluster_size;
        let off = if let Some(reused) = self.free_clusters.as_mut().and_then(|v| v.pop()) {
            reused
        } else {
            let new_off = self.host_end;
            self.host_end += cluster_size;
            new_off
        };
        if zero_fill {
            self.inner.seek(SeekFrom::Start(off))?;
            let zeros = vec![0u8; cluster_size as usize];
            self.inner.write_all(&zeros)?;
        }
        self.set_refcount_one(off / cluster_size)?;
        Ok(off)
    }

    /// Set refcount[cluster_idx] = 1, growing the refcount **block** at the
    /// covering table slot on demand. The new block is itself a host cluster
    /// that needs `refcount=1` — handled via a one-deep recursion since the
    /// table slot is populated by the time we call back in. Refuses to grow
    /// the refcount **table** itself (capacity = `refcount_table_capacity`),
    /// which would mean relocating the entire structure; at typical 64 KiB
    /// clusters one table cluster already addresses ~16 TiB of host data.
    fn set_refcount_one(&mut self, cluster_idx: u64) -> std::io::Result<()> {
        let cluster_size = self.header.cluster_size;
        let refcounts_per_block = cluster_size / 2;
        let table_idx = (cluster_idx / refcounts_per_block) as usize;
        let entry_idx = cluster_idx % refcounts_per_block;

        if table_idx >= self.refcount_table_capacity {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!(
                    "QCOW2 edit would need to grow the refcount *table* (slot \
                     {table_idx} of {}); not supported",
                    self.refcount_table_capacity
                ),
            ));
        }

        if self.refcount_table[table_idx] == 0 {
            // Refcount-block growth: append a fresh zero-filled cluster, patch
            // the on-disk refcount table, then recurse to set the new block's
            // own refcount=1 (its table slot is now populated, so the recursion
            // terminates after one more call).
            let new_block_off = self.host_end;
            self.host_end += cluster_size;
            self.inner.seek(SeekFrom::Start(new_block_off))?;
            let zeros = vec![0u8; cluster_size as usize];
            self.inner.write_all(&zeros)?;
            self.refcount_table[table_idx] = new_block_off;
            let rt_entry_off = self.refcount_table_offset + (table_idx as u64) * 8;
            self.inner.seek(SeekFrom::Start(rt_entry_off))?;
            self.inner.write_all(&new_block_off.to_be_bytes())?;
            self.set_refcount_one(new_block_off / cluster_size)?;
        }

        let rb_off = self.refcount_table[table_idx];
        self.inner.seek(SeekFrom::Start(rb_off + entry_idx * 2))?;
        self.inner.write_all(&1u16.to_be_bytes())?;
        Ok(())
    }

    /// Lazily build the free-cluster index on the first edit. Scans every
    /// populated refcount block once and records host clusters whose refcount
    /// is zero AND whose offset is below `host_end` (i.e. inside the physical
    /// file — never going above EOF, since clusters past EOF don't exist
    /// yet). Subsequent allocations drain this Vec via `pop()` before
    /// extending the file.
    ///
    /// Cost: one pass over the existing refcount blocks (at 64 KiB clusters /
    /// 16-bit refcounts, that's 2 bytes per host cluster — a few MiB even on
    /// multi-TB images, comfortably one-shot).
    fn ensure_free_cluster_index(&mut self) -> std::io::Result<()> {
        if self.free_clusters.is_some() {
            return Ok(());
        }
        let cluster_size = self.header.cluster_size;
        let refcounts_per_block = cluster_size / 2;
        let total_clusters_in_file = self.host_end / cluster_size;
        let mut free: Vec<u64> = Vec::new();
        let rt_snapshot: Vec<u64> = self.refcount_table.clone();
        for (table_idx, &rb_off) in rt_snapshot.iter().enumerate() {
            if rb_off == 0 {
                continue;
            }
            let block_start_cluster = (table_idx as u64) * refcounts_per_block;
            if block_start_cluster >= total_clusters_in_file {
                break;
            }
            let block_end_cluster =
                ((table_idx as u64 + 1) * refcounts_per_block).min(total_clusters_in_file);
            let entries_in_use = (block_end_cluster - block_start_cluster) as usize;
            let mut buf = vec![0u8; entries_in_use * 2];
            self.inner.seek(SeekFrom::Start(rb_off))?;
            self.inner.read_exact(&mut buf)?;
            for (i, chunk) in buf.chunks_exact(2).enumerate() {
                let refcount = u16::from_be_bytes(chunk.try_into().unwrap());
                if refcount == 0 {
                    let cluster_idx = block_start_cluster + i as u64;
                    free.push(cluster_idx * cluster_size);
                }
            }
        }
        // Reverse so `pop()` hands out the lowest free offset first — locality,
        // and matches the order qemu-img's leak detector would have us
        // reclaim.
        free.reverse();
        self.free_clusters = Some(free);
        Ok(())
    }

    /// Patch one L1 entry in place on disk (BE u64 at `l1_table_offset + 8*idx`).
    fn write_l1_entry(&mut self, l1_idx: usize) -> std::io::Result<()> {
        let off = self.l1_table_offset + (l1_idx as u64) * 8;
        self.inner.seek(SeekFrom::Start(off))?;
        self.inner.write_all(&self.l1[l1_idx].to_be_bytes())?;
        Ok(())
    }

    /// Read one L2 entry straight from disk (no cache).
    fn read_l2_entry_raw(&mut self, l2_host: u64, l2_idx: usize) -> std::io::Result<u64> {
        let off = l2_host + (l2_idx as u64) * 8;
        self.inner.seek(SeekFrom::Start(off))?;
        let mut buf = [0u8; 8];
        self.inner.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }

    /// Patch one L2 entry in place (BE u64 at `l2_host + 8*idx`).
    fn write_l2_entry_raw(
        &mut self,
        l2_host: u64,
        l2_idx: usize,
        entry: u64,
    ) -> std::io::Result<()> {
        let off = l2_host + (l2_idx as u64) * 8;
        self.inner.seek(SeekFrom::Start(off))?;
        self.inner.write_all(&entry.to_be_bytes())?;
        Ok(())
    }
}

/// Stream a `disk_size`-byte source into a QCOW2 v3 image on `out`.
///
/// Allocates a host cluster only for non-zero source clusters (zero clusters
/// stay unmapped → read back as zeros). L2 tables and refcount blocks are
/// allocated lazily / pre-sized respectively; the writer emits a fully valid
/// uncompressed v3 image with `refcount_order = 4` that `qemu-img check`
/// accepts.
///
/// `cluster_bits == 0` selects [`QCOW2_DEFAULT_CLUSTER_BITS`] (16 → 64 KiB).
/// Otherwise it must fall in `9..=21` (512 B .. 2 MiB).
///
/// Layout written (host byte offsets, all multi-byte fields big-endian):
///
/// ```text
/// +0                            v3 header (112 B), zero-padded to one cluster
/// +cluster_size                 L1 table  (l1_size * u64, padded to cluster_size)
/// +<after L1>                   Refcount table (1 cluster; u64 offsets pointing
///                               at each refcount block)
/// +<after refcount table>       Refcount blocks (pre-reserved at file build time
///                               so layout decisions are independent of source
///                               content; sized to cover every host cluster)
/// +<after refcount blocks>      L2 tables and data clusters, allocated as the
///                               source is streamed through. Each L2 table or
///                               data cluster is one cluster.
/// ```
///
/// `progress_cb` is fed cumulative source bytes consumed (matches the dynamic
/// VHD writer's contract); `cancel_check` is polled per source cluster.
pub fn export_qcow2(
    reader: &mut impl Read,
    out: &mut (impl Write + Seek),
    disk_size: u64,
    cluster_bits: u32,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
) -> Result<()> {
    use super::sparse::{is_zero_unit, SparseAllocator};
    use std::collections::BTreeMap;

    let cluster_bits = if cluster_bits == 0 {
        QCOW2_DEFAULT_CLUSTER_BITS
    } else {
        cluster_bits
    };
    if !(9..=21).contains(&cluster_bits) {
        bail!("QCOW2 cluster_bits {cluster_bits} out of supported range 9..=21");
    }
    let cluster_size = 1u64 << cluster_bits;
    let entries_per_l2 = cluster_size / 8;
    let num_guest_clusters = disk_size.div_ceil(cluster_size);
    // l1_size always >= 1 so qemu-img check sees a non-empty L1 even for the
    // 0-byte degenerate case.
    let l1_size = disk_size.div_ceil(cluster_size * entries_per_l2).max(1);

    // Layout: prefix regions sized up-front (header, L1, refcount-table) live
    // at fixed offsets so we never seek-and-grow during streaming. The
    // refcount **blocks** are allocated *after* streaming so we emit exactly
    // the count we need — pre-reserving worst-case blocks (as the dynamic
    // VHD writer can with its BAT) makes `qemu-img check` flag the extras as
    // "leaked clusters" because they're allocated but unreferenced.
    let l1_offset = cluster_size;
    let l1_bytes_padded = (l1_size * 8).div_ceil(cluster_size) * cluster_size;
    let refcount_table_offset = l1_offset + l1_bytes_padded;
    // refcount_order = 4 → 2 bytes per entry → cluster_size/2 entries per block.
    let refcounts_per_block = cluster_size / 2;
    let data_start = refcount_table_offset + cluster_size;

    // Zero-fill the reserved prefix so unused L1 slots / refcount-table entries
    // read as 0 (= "no allocation") rather than uninitialised tempfile garbage.
    let zero_cluster = vec![0u8; cluster_size as usize];
    out.seek(SeekFrom::Start(0))?;
    let mut written = 0u64;
    while written < data_start {
        out.write_all(&zero_cluster)
            .context("failed to zero-fill QCOW2 reserved region")?;
        written += cluster_size;
    }

    let mut alloc = SparseAllocator::new(data_start, cluster_size);
    let mut l1: Vec<u64> = vec![0; l1_size as usize];
    // L2 tables held in RAM: l1_idx -> (host offset, l2 entries).
    let mut l2_tables: BTreeMap<u32, (u64, Vec<u64>)> = BTreeMap::new();

    let mut buf = vec![0u8; cluster_size as usize];
    for guest_idx in 0..num_guest_clusters {
        if cancel_check() {
            bail!("QCOW2 export cancelled");
        }
        let guest_off = guest_idx * cluster_size;
        let to_read = (disk_size - guest_off).min(cluster_size) as usize;
        reader
            .read_exact(&mut buf[..to_read])
            .context("failed to read source cluster")?;
        for b in buf[to_read..].iter_mut() {
            *b = 0;
        }

        if !is_zero_unit(&buf[..to_read]) {
            let l1_idx = (guest_idx / entries_per_l2) as u32;
            let l2_idx = (guest_idx % entries_per_l2) as usize;
            if let std::collections::btree_map::Entry::Vacant(e) = l2_tables.entry(l1_idx) {
                // Allocate a fresh L2 table cluster, zero it on disk so unset
                // slots read as 0, and point L1 at it.
                let l2_off = alloc.alloc();
                out.seek(SeekFrom::Start(l2_off))?;
                out.write_all(&zero_cluster)
                    .context("failed to zero-fill new L2 cluster")?;
                l1[l1_idx as usize] = l2_off | QCOW2_FLAG_COPIED;
                e.insert((l2_off, vec![0u64; entries_per_l2 as usize]));
            }
            let data_off = alloc.alloc();
            out.seek(SeekFrom::Start(data_off))?;
            out.write_all(&buf)
                .context("failed to write data cluster")?;
            let (_, l2_entries) = l2_tables.get_mut(&l1_idx).unwrap();
            l2_entries[l2_idx] = data_off | QCOW2_FLAG_COPIED;
        }
        progress_cb(guest_off + to_read as u64);
    }

    // Compute exactly how many refcount blocks we need. Self-referential: each
    // block we add is a host cluster that needs its own refcount entry, so we
    // iterate until the count is stable. Converges in O(log) because every
    // added block can address `refcounts_per_block` additional clusters.
    let post_stream_end = alloc.next_offset();
    let mut needed_refcount_blocks: u64 = 1;
    loop {
        let total_with_blocks =
            (post_stream_end + needed_refcount_blocks * cluster_size) / cluster_size;
        let recomputed = total_with_blocks.div_ceil(refcounts_per_block).max(1);
        if recomputed <= needed_refcount_blocks {
            break;
        }
        needed_refcount_blocks = recomputed;
    }
    // Alloc + zero-fill each refcount block at the current end of the file.
    let mut refcount_block_offsets: Vec<u64> = Vec::with_capacity(needed_refcount_blocks as usize);
    for _ in 0..needed_refcount_blocks {
        let off = alloc.alloc();
        out.seek(SeekFrom::Start(off))?;
        out.write_all(&zero_cluster)
            .context("failed to zero-fill refcount block")?;
        refcount_block_offsets.push(off);
    }
    let final_end = alloc.next_offset();
    let total_host_clusters = final_end / cluster_size;

    // Backfill L2 tables in place.
    for (l2_off, l2_entries) in l2_tables.values() {
        let mut bytes = Vec::with_capacity(l2_entries.len() * 8);
        for e in l2_entries {
            bytes.extend_from_slice(&e.to_be_bytes());
        }
        out.seek(SeekFrom::Start(*l2_off))?;
        out.write_all(&bytes).context("failed to write L2 table")?;
    }

    // Backfill L1 table.
    out.seek(SeekFrom::Start(l1_offset))?;
    let mut l1_bytes_buf = Vec::with_capacity(l1.len() * 8);
    for e in &l1 {
        l1_bytes_buf.extend_from_slice(&e.to_be_bytes());
    }
    out.write_all(&l1_bytes_buf)
        .context("failed to write L1 table")?;

    // Refcount blocks: every host cluster 0..total_host_clusters has refcount=1.
    // Unused tail entries inside the last block stay zero (unallocated).
    for (block_idx, block_off) in refcount_block_offsets.iter().enumerate() {
        let block_idx = block_idx as u64;
        let mut block = vec![0u8; cluster_size as usize];
        let first_cluster = block_idx * refcounts_per_block;
        let last_cluster = ((block_idx + 1) * refcounts_per_block).min(total_host_clusters);
        for cluster in first_cluster..last_cluster {
            let entry_off = ((cluster - first_cluster) * 2) as usize;
            block[entry_off..entry_off + 2].copy_from_slice(&1u16.to_be_bytes());
        }
        out.seek(SeekFrom::Start(*block_off))?;
        out.write_all(&block)
            .context("failed to write refcount block")?;
    }

    // Refcount table: one u64 BE per refcount block.
    out.seek(SeekFrom::Start(refcount_table_offset))?;
    let mut rt = Vec::with_capacity(refcount_block_offsets.len() * 8);
    for off in &refcount_block_offsets {
        rt.extend_from_slice(&off.to_be_bytes());
    }
    out.write_all(&rt)
        .context("failed to write refcount table")?;

    // Header at the very end so it isn't there until everything else is valid.
    out.seek(SeekFrom::Start(0))?;
    let header = build_qcow2_v3_header(
        cluster_bits,
        disk_size,
        l1_size as u32,
        l1_offset,
        refcount_table_offset,
        1,
    );
    out.write_all(&header)
        .context("failed to write QCOW2 header")?;

    out.flush()?;
    Ok(())
}

/// Build the 112-byte v3 header (104-byte declared length + 8 bytes of
/// compression_type + zero padding to 16-byte alignment, matching `qemu-img`).
/// Feature words and `refcount_order` follow `compat=1.1` defaults; refcount
/// table size is `refcount_table_clusters * cluster_size` bytes.
fn build_qcow2_v3_header(
    cluster_bits: u32,
    size: u64,
    l1_size: u32,
    l1_offset: u64,
    refcount_table_offset: u64,
    refcount_table_clusters: u32,
) -> Vec<u8> {
    let mut h = vec![0u8; 112];
    h[0..4].copy_from_slice(QCOW2_MAGIC);
    h[4..8].copy_from_slice(&3u32.to_be_bytes());
    // backing_file_offset (8..16) + backing_file_size (16..20) = 0
    h[20..24].copy_from_slice(&cluster_bits.to_be_bytes());
    h[24..32].copy_from_slice(&size.to_be_bytes());
    // crypt_method (32..36) = 0
    h[36..40].copy_from_slice(&l1_size.to_be_bytes());
    h[40..48].copy_from_slice(&l1_offset.to_be_bytes());
    h[48..56].copy_from_slice(&refcount_table_offset.to_be_bytes());
    h[56..60].copy_from_slice(&refcount_table_clusters.to_be_bytes());
    // nb_snapshots (60..64), snapshots_offset (64..72) = 0
    // incompatible/compatible/autoclear (72..96) = 0
    h[96..100].copy_from_slice(&4u32.to_be_bytes()); // refcount_order
    h[100..104].copy_from_slice(&QCOW2_V3_HEADER_LEN.to_be_bytes());
    // 104: compression_type = 0 (zlib, the only spec-defined value when the
    // compression-type incompatible bit is clear, which it is for us).
    // 105..112: zero padding to the next 16-byte boundary.
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal in-memory QCOW2 image with the given parameters and an
    /// optional set of (guest_cluster_index, payload) pairs. Each payload is
    /// `cluster_size` bytes; missing clusters stay unallocated. The image gets
    /// one L2 table at most (all writes share L1 slot 0 in these tests).
    ///
    /// Layout in the produced buffer:
    /// ```
    /// +0                       header (104 B padded to one cluster)
    /// +cluster_size            L1 table (1 cluster)
    /// +2*cluster_size          L2 table (1 cluster)
    /// +3*cluster_size          first data cluster, etc.
    /// ```
    /// The refcount structures are not emitted — the reader never consults them.
    fn build_minimal_qcow2(
        version: u32,
        cluster_bits: u32,
        disk_size: u64,
        zero_flag_clusters: &[u64],
        data_clusters: &[(u64, Vec<u8>)],
    ) -> Vec<u8> {
        let cluster_size = 1u64 << cluster_bits;
        let entries_per_l2 = cluster_size / 8;
        for (i, _) in data_clusters {
            assert!(
                *i < entries_per_l2,
                "test helper only emits one L2 table; all clusters must share L1 slot 0"
            );
        }

        let l1_table_offset = cluster_size;
        let l2_table_offset = 2 * cluster_size;
        let data_region_offset = 3 * cluster_size;

        let total_data_clusters = data_clusters.len() as u64;
        let total_bytes = data_region_offset + total_data_clusters * cluster_size;
        let mut buf = vec![0u8; total_bytes as usize];

        // Header.
        buf[0..4].copy_from_slice(QCOW2_MAGIC);
        buf[4..8].copy_from_slice(&version.to_be_bytes());
        // backing_file_offset (8..16) = 0
        buf[20..24].copy_from_slice(&cluster_bits.to_be_bytes());
        buf[24..32].copy_from_slice(&disk_size.to_be_bytes());
        // crypt_method (32..36) = 0
        // l1_size: just enough to cover disk_size.
        let required_l1 = disk_size.div_ceil(cluster_size * entries_per_l2);
        let l1_size = required_l1.max(1) as u32;
        buf[36..40].copy_from_slice(&l1_size.to_be_bytes());
        buf[40..48].copy_from_slice(&l1_table_offset.to_be_bytes());
        // refcount_table_offset / clusters (48..60) and nb_snapshots (60..64) = 0
        if version == 3 {
            // incompatible/compatible/autoclear (72..96) = 0
            buf[96..100].copy_from_slice(&4u32.to_be_bytes()); // refcount_order
            buf[100..104].copy_from_slice(&104u32.to_be_bytes()); // header_length
        }

        // L1[0] = l2_table_offset | COPIED.
        let l1_entry = l2_table_offset | QCOW2_FLAG_COPIED;
        buf[l1_table_offset as usize..l1_table_offset as usize + 8]
            .copy_from_slice(&l1_entry.to_be_bytes());

        // L2 entries + data clusters.
        for (slot, (idx, payload)) in data_clusters.iter().enumerate() {
            assert_eq!(payload.len() as u64, cluster_size);
            let host = data_region_offset + slot as u64 * cluster_size;
            let entry = host | QCOW2_FLAG_COPIED;
            let l2_entry_off = (l2_table_offset + *idx * 8) as usize;
            buf[l2_entry_off..l2_entry_off + 8].copy_from_slice(&entry.to_be_bytes());
            buf[host as usize..(host + cluster_size) as usize].copy_from_slice(payload);
        }
        for idx in zero_flag_clusters {
            // QCOW_OFLAG_ZERO only honoured on v3; tests use v3 when exercising it.
            let entry = QCOW2_FLAG_ZERO;
            let l2_entry_off = (l2_table_offset + *idx * 8) as usize;
            buf[l2_entry_off..l2_entry_off + 8].copy_from_slice(&entry.to_be_bytes());
        }
        buf
    }

    #[test]
    fn round_trip_v3_allocated_and_sparse() {
        let cluster_bits = 9; // 512 B clusters — keep test image tiny.
        let cluster_size = 1u64 << cluster_bits;
        // Three clusters: 0 allocated with 0xAA pattern, 1 unallocated → zeros,
        // 2 allocated with 0x55 pattern.
        let pattern_aa = vec![0xAAu8; cluster_size as usize];
        let pattern_55 = vec![0x55u8; cluster_size as usize];
        let disk_size = 3 * cluster_size;
        let img = build_minimal_qcow2(
            3,
            cluster_bits,
            disk_size,
            &[],
            &[(0, pattern_aa.clone()), (2, pattern_55.clone())],
        );

        let mut r = Qcow2Reader::open(Cursor::new(img)).unwrap();
        assert_eq!(r.len(), disk_size);
        assert_eq!(r.version(), 3);

        let mut out = vec![0u8; disk_size as usize];
        let mut filled = 0;
        while filled < out.len() {
            let n = r.read(&mut out[filled..]).unwrap();
            assert!(n > 0, "premature EOF");
            filled += n;
        }
        assert_eq!(&out[..cluster_size as usize], &pattern_aa[..]);
        assert!(out[cluster_size as usize..2 * cluster_size as usize]
            .iter()
            .all(|&b| b == 0));
        assert_eq!(&out[2 * cluster_size as usize..], &pattern_55[..]);
    }

    #[test]
    fn read_never_crosses_cluster_boundary() {
        let cluster_bits = 9;
        let cluster_size = 1u64 << cluster_bits;
        let payload = vec![0x11u8; cluster_size as usize];
        let img = build_minimal_qcow2(
            3,
            cluster_bits,
            2 * cluster_size,
            &[],
            &[(0, payload.clone())],
        );
        let mut r = Qcow2Reader::open(Cursor::new(img)).unwrap();
        // Seek to the last 16 bytes of cluster 0 and request 64 bytes — only 16
        // should come back (boundary stops the read).
        r.seek(SeekFrom::Start(cluster_size - 16)).unwrap();
        let mut buf = [0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(n, 16);
        assert!(buf[..16].iter().all(|&b| b == 0x11));
    }

    #[test]
    fn v3_zero_flag_reads_as_zeros() {
        let cluster_bits = 9;
        let cluster_size = 1u64 << cluster_bits;
        let img = build_minimal_qcow2(3, cluster_bits, 2 * cluster_size, &[1], &[]);
        let mut r = Qcow2Reader::open(Cursor::new(img)).unwrap();
        r.seek(SeekFrom::Start(cluster_size)).unwrap();
        let mut buf = vec![0xFFu8; cluster_size as usize];
        r.read_exact(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn v2_zero_flag_bit_is_ignored() {
        // On v2 the low bit of an L2 entry is reserved, not QCOW_OFLAG_ZERO.
        // Build a v2 image where L2[0] has bit 0 set but no host offset; the
        // reader should treat host == 0 as unallocated (also zeros — but the
        // codepath is different and we want to make sure we don't accidentally
        // honour the flag on v2).
        let cluster_bits = 9;
        let cluster_size = 1u64 << cluster_bits;
        let img = build_minimal_qcow2(2, cluster_bits, cluster_size, &[0], &[]);
        let mut r = Qcow2Reader::open(Cursor::new(img)).unwrap();
        assert_eq!(r.version(), 2);
        let mut buf = vec![0xFFu8; cluster_size as usize];
        r.read_exact(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn round_trip_v2_allocated() {
        let cluster_bits = 9;
        let cluster_size = 1u64 << cluster_bits;
        let payload = vec![0x77u8; cluster_size as usize];
        let img = build_minimal_qcow2(2, cluster_bits, cluster_size, &[], &[(0, payload.clone())]);
        let mut r = Qcow2Reader::open(Cursor::new(img)).unwrap();
        assert_eq!(r.version(), 2);
        let mut buf = vec![0u8; cluster_size as usize];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(buf, payload);
    }

    /// Open and expect failure; returns the error so the caller can assert on
    /// its message. Qcow2Reader doesn't implement `Debug` (its inner `R` may
    /// not), so we can't use `unwrap_err`.
    fn open_expecting_err(img: Vec<u8>) -> anyhow::Error {
        match Qcow2Reader::open(Cursor::new(img)) {
            Ok(_) => panic!("expected Qcow2Reader::open to fail"),
            Err(e) => e,
        }
    }

    #[test]
    fn rejects_qcow1() {
        // Magic + version=1 is enough for the reader to refuse without parsing
        // anything else.
        let mut buf = vec![0u8; 72];
        buf[0..4].copy_from_slice(QCOW2_MAGIC);
        buf[4..8].copy_from_slice(&1u32.to_be_bytes());
        let err = open_expecting_err(buf);
        assert!(err.to_string().contains("qcow1"));
    }

    #[test]
    fn rejects_unsupported_header_features() {
        let cluster_bits = 9;
        // (byte offset, field width, big-endian value, expected error substring).
        // The 4-byte fields are crypt_method@32 / nb_snapshots@60; the 8-byte
        // fields are incompatible_features@72 / backing_file_offset@8.
        let cases: &[(usize, usize, u64, &str)] = &[
            (32, 4, 1, "encrypted"),
            (60, 4, 3, "snapshot"),
            (72, 8, 1 << 5, "unknown incompatible_features"),
            (72, 8, 1 << 2, "external data file"),
            (72, 8, 1 << 4, "extended L2"),
            (8, 8, 0xDEAD_BEEF, "backing file"),
        ];
        for (off, width, value, msg) in cases {
            let mut img = build_minimal_qcow2(3, cluster_bits, 1u64 << cluster_bits, &[], &[]);
            let be = value.to_be_bytes();
            img[*off..*off + *width].copy_from_slice(&be[8 - *width..]);
            let err = open_expecting_err(img);
            assert!(
                err.to_string().contains(*msg),
                "expected {msg:?}, got: {err}"
            );
        }
    }

    #[test]
    fn rejects_compressed_cluster_on_read() {
        let cluster_bits = 9;
        let cluster_size = 1u64 << cluster_bits;
        let mut img = build_minimal_qcow2(
            3,
            cluster_bits,
            cluster_size,
            &[],
            &[(0, vec![0u8; cluster_size as usize])],
        );
        // Stamp L2[0] with the compressed bit on top of its current entry.
        let l2_off = (2 * cluster_size) as usize;
        let existing = u64::from_be_bytes(img[l2_off..l2_off + 8].try_into().unwrap());
        let with_compressed = existing | QCOW2_FLAG_COMPRESSED;
        img[l2_off..l2_off + 8].copy_from_slice(&with_compressed.to_be_bytes());

        let mut r = Qcow2Reader::open(Cursor::new(img)).unwrap();
        let mut buf = [0u8; 16];
        let err = r.read(&mut buf).unwrap_err();
        assert!(err.to_string().contains("compressed"));
    }

    // -- Writer tests ------------------------------------------------------

    /// Build a deterministic pseudo-random source of `len` bytes that's neither
    /// all-zero (so the writer actually allocates clusters) nor degenerate
    /// (so a wrong off-by-one in the address math shows up).
    fn make_pattern(len: usize) -> Vec<u8> {
        let mut v = vec![0u8; len];
        for (i, b) in v.iter_mut().enumerate() {
            *b = (i.wrapping_mul(31337) ^ (i >> 3)) as u8;
        }
        v
    }

    /// Read the QCOW2 image at `cur` back through `Qcow2Reader` and compare
    /// against `expected`. Helper used by every round-trip writer test.
    fn assert_qcow2_round_trips(cur: Cursor<Vec<u8>>, expected: &[u8]) {
        let mut r = Qcow2Reader::open(cur).unwrap();
        assert_eq!(r.len(), expected.len() as u64);
        let mut out = vec![0u8; expected.len()];
        r.read_exact(&mut out).unwrap();
        assert_eq!(out, expected, "QCOW2 round-trip mismatch");
    }

    #[test]
    fn writer_round_trips_dense_disk() {
        // 4 KiB disk, 512 B clusters: 8 guest clusters, all non-zero so every
        // one gets allocated. Exercises L1[0] → L2 → 8 data clusters.
        let disk_size = 4096u64;
        let src = make_pattern(disk_size as usize);
        let mut out = Cursor::new(Vec::new());
        export_qcow2(
            &mut Cursor::new(&src),
            &mut out,
            disk_size,
            9,
            &mut |_| {},
            &|| false,
        )
        .unwrap();
        assert_qcow2_round_trips(out, &src);
    }

    #[test]
    fn writer_round_trips_sparse_disk() {
        // 4 KiB disk with two non-zero clusters surrounded by zeros. The zero
        // clusters must read back as zero without ever being allocated.
        let cs = 512usize;
        let disk = 8 * cs;
        let mut src = vec![0u8; disk];
        let pat0 = make_pattern(cs);
        let pat3 = vec![0xC3u8; cs];
        src[0..cs].copy_from_slice(&pat0);
        src[3 * cs..4 * cs].copy_from_slice(&pat3);
        let mut out = Cursor::new(Vec::new());
        export_qcow2(
            &mut Cursor::new(&src),
            &mut out,
            disk as u64,
            9,
            &mut |_| {},
            &|| false,
        )
        .unwrap();
        // Output should be significantly smaller than the disk — only two data
        // clusters allocated plus metadata.
        assert!(
            (out.get_ref().len() as u64) < disk as u64,
            "sparse output not smaller than disk: {} vs {}",
            out.get_ref().len(),
            disk,
        );
        assert_qcow2_round_trips(out, &src);
    }

    #[test]
    fn writer_all_zero_input_emits_no_data_clusters() {
        // 64 KiB all-zero source. Output must be just the metadata prefix
        // (header + L1 + refcount table + refcount block — no L2, no data).
        let cs = 512usize;
        let disk = 128 * cs; // 64 KiB
        let src = vec![0u8; disk];
        let mut out = Cursor::new(Vec::new());
        export_qcow2(
            &mut Cursor::new(&src),
            &mut out,
            disk as u64,
            9,
            &mut |_| {},
            &|| false,
        )
        .unwrap();
        // Exact upper bound: header + L1(1) + RT(1) + max_refcount_blocks(>=1).
        // With cluster=512, max_clusters_no_refcount ≈ 132, one block (256
        // entries) is enough → 4 clusters total = 2048 bytes.
        assert_eq!(out.get_ref().len() as u64, 4 * cs as u64);
        assert_qcow2_round_trips(out, &src);
    }

    #[test]
    fn writer_default_cluster_bits() {
        // Passing cluster_bits == 0 selects the 64 KiB default. Use a small
        // disk so the test runs quickly; just verify round-trip + that the
        // emitted image's cluster_bits reports as 16.
        let disk_size = 64 * 1024u64;
        let src = make_pattern(disk_size as usize);
        let mut out = Cursor::new(Vec::new());
        export_qcow2(
            &mut Cursor::new(&src),
            &mut out,
            disk_size,
            0,
            &mut |_| {},
            &|| false,
        )
        .unwrap();
        // Inspect cluster_bits directly from the emitted header.
        assert_eq!(
            u32::from_be_bytes(out.get_ref()[20..24].try_into().unwrap()),
            QCOW2_DEFAULT_CLUSTER_BITS
        );
        assert_qcow2_round_trips(out, &src);
    }

    #[test]
    fn writer_cancel_aborts() {
        let disk_size = 4096u64;
        let src = make_pattern(disk_size as usize);
        let mut out = Cursor::new(Vec::new());
        let err = export_qcow2(
            &mut Cursor::new(&src),
            &mut out,
            disk_size,
            9,
            &mut |_| {},
            &|| true,
        )
        .unwrap_err();
        assert!(err.to_string().contains("cancelled"));
    }

    // -- In-place edit tests (Session 2.3) --------------------------------

    /// Build a writer-produced QCOW2 image into a `Vec<u8>`, returning the
    /// reader (positioned at offset 0). Tests for the editor reuse this so the
    /// "fresh image" they edit is one we know is `qemu-img check`-clean.
    fn fresh_qcow2(disk_size: u64, cluster_bits: u32, fill: &[u8]) -> Cursor<Vec<u8>> {
        let mut src = vec![0u8; disk_size as usize];
        src[..fill.len()].copy_from_slice(fill);
        let mut out = Cursor::new(Vec::new());
        export_qcow2(
            &mut Cursor::new(&src),
            &mut out,
            disk_size,
            cluster_bits,
            &mut |_| {},
            &|| false,
        )
        .unwrap();
        out.set_position(0);
        out
    }

    #[test]
    fn editor_in_place_update_of_allocated_cluster() {
        // Build a fresh image with cluster 0 non-zero, then overwrite the
        // first 16 bytes of guest cluster 0 in place. The host cluster stays
        // the same — no allocation, no L1/L2 mutation.
        let cs = 512u64;
        let backing = fresh_qcow2(2 * cs, 9, &[0x11u8; 16]);
        let mut e = Qcow2Reader::open(backing).unwrap();
        let host_end_before = e.host_end;
        e.seek(SeekFrom::Start(0)).unwrap();
        e.write_all(&[0xAB; 16]).unwrap();
        // No new host cluster should have been allocated.
        assert_eq!(e.host_end, host_end_before);
        // Re-read via the same instance.
        e.seek(SeekFrom::Start(0)).unwrap();
        let mut read_back = [0u8; 16];
        e.read_exact(&mut read_back).unwrap();
        assert_eq!(read_back, [0xAB; 16]);
        // And via a fresh reader over the same bytes — invalidation works.
        let bytes = e.inner.into_inner();
        let mut r2 = Qcow2Reader::open(Cursor::new(bytes)).unwrap();
        let mut buf = [0u8; 16];
        r2.read_exact(&mut buf).unwrap();
        assert_eq!(buf, [0xAB; 16]);
    }

    #[test]
    fn editor_allocates_data_cluster_for_sparse_write() {
        // Fresh image with cluster 0 allocated, cluster 1 sparse. Writing into
        // cluster 1 must allocate a new host cluster and patch the L2 entry,
        // without touching the L1 table (L2 already exists for L1[0]).
        let cs = 512u64;
        let backing = fresh_qcow2(2 * cs, 9, &[0xCDu8; 16]);
        let mut e = Qcow2Reader::open(backing).unwrap();
        let host_end_before = e.host_end;
        e.seek(SeekFrom::Start(cs)).unwrap();
        let payload = [0xEFu8; 32];
        e.write_all(&payload).unwrap();
        assert_eq!(
            e.host_end,
            host_end_before + cs,
            "exactly one cluster allocated for the new data block"
        );
        // Round-trip via a fresh reader.
        let bytes = e.inner.into_inner();
        let mut r2 = Qcow2Reader::open(Cursor::new(bytes)).unwrap();
        r2.seek(SeekFrom::Start(cs)).unwrap();
        let mut out = vec![0u8; cs as usize];
        r2.read_exact(&mut out).unwrap();
        assert_eq!(&out[..32], &payload);
        // Tail of the cluster stays zero — partial writes don't leak garbage.
        assert!(out[32..].iter().all(|&b| b == 0));
    }

    #[test]
    fn editor_grows_l2_for_new_l1_slot() {
        // Cluster size 512 → entries_per_l2 = 64 → guest cluster 64 falls into
        // L1 slot 1. We need a disk big enough to have at least 2 L1 slots
        // worth of capacity (cluster_size * entries_per_l2 * 2 = 64 KiB).
        let cs = 512u64;
        let disk = 64 * 1024u64;
        // Pre-fill only the very first cluster so L1[0] gets an L2 table but
        // L1[1] stays empty (l2_host = 0).
        let backing = fresh_qcow2(disk, 9, &[0xAA; 16]);
        let mut e = Qcow2Reader::open(backing).unwrap();
        assert_eq!(e.l1[1], 0, "test setup needs L1[1] to start empty");
        let host_end_before = e.host_end;
        // Write into guest cluster 64 — L1 slot 1's first L2 entry.
        let target = 64 * cs;
        e.seek(SeekFrom::Start(target)).unwrap();
        let payload = [0xC3u8; 24];
        e.write_all(&payload).unwrap();
        // Expect TWO clusters allocated: a fresh L2 table and a fresh data
        // cluster.
        assert_eq!(e.host_end, host_end_before + 2 * cs);
        assert_ne!(e.l1[1], 0, "L1[1] must now point at a new L2");
        assert!(
            e.l1[1] & QCOW2_FLAG_COPIED != 0,
            "newly-allocated L1 entry should set COPIED"
        );
        // Reopen and verify the write is visible + the partial cluster's tail
        // stayed zero.
        let bytes = e.inner.into_inner();
        let mut r2 = Qcow2Reader::open(Cursor::new(bytes)).unwrap();
        r2.seek(SeekFrom::Start(target)).unwrap();
        let mut out = vec![0u8; cs as usize];
        r2.read_exact(&mut out).unwrap();
        assert_eq!(&out[..24], &payload);
        assert!(out[24..].iter().all(|&b| b == 0));
    }

    #[test]
    fn editor_grows_refcount_block_when_appending_past_first_block() {
        // 512 B clusters → refcounts_per_block = 256 → one refcount block
        // covers 256 host clusters = 128 KiB host data. Build a small fresh
        // image (a few KB), then write enough new data clusters to overflow
        // the first refcount block. The editor must allocate a fresh refcount
        // block, patch refcount_table[1], and the result must pass qemu-img
        // check.
        let cs = 512u64;
        // Disk must be big enough to demand the writes that overflow.
        // refcounts_per_block = 256; image starts with ~4 clusters allocated
        // (header, L1, rt, rb0). Writing into ~270 distinct guest clusters
        // pushes us past the first refcount block's 256-cluster reach.
        let disk = 512 * cs; // 256 KiB virtual disk → 512 guest clusters
        let backing = fresh_qcow2(disk, 9, &[0xAA; 16]);
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), backing.get_ref()).unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            let mut e = Qcow2Reader::open(f).unwrap();
            // Refcount table must have at least 2 slots so we can grow into the
            // second one. With this image size the writer emits exactly one
            // refcount-table cluster (capacity = 64 slots @ 512 B cluster_size
            // = cluster_size / 8) — plenty of headroom.
            assert!(
                e.refcount_table_capacity >= 2,
                "test premise needs >=2 refcount table slots, got {}",
                e.refcount_table_capacity
            );
            // Write into ~300 guest clusters, each in its own cluster, to
            // force enough data-cluster allocations to push the host file past
            // cluster index 256.
            for guest_idx in 0..300u64 {
                e.seek(SeekFrom::Start(guest_idx * cs)).unwrap();
                let pattern = [(guest_idx as u8).wrapping_add(1); 8];
                e.write_all(&pattern).unwrap();
            }
            // After all those writes refcount_table[1] must be populated.
            assert!(
                e.refcount_table.get(1).copied().unwrap_or(0) != 0,
                "refcount block growth never happened (rt[1] still 0)"
            );
            e.flush().unwrap();
        }
        match crate::rbformats::qemu_img_test::qemu_img_check(tmp.path()) {
            Ok(true) => {}
            Ok(false) => eprintln!("skipping qemu-img check — `qemu-img` not on PATH"),
            Err(e) => panic!("qemu-img check failed after refcount-block growth: {e}"),
        }
        // Read-back check via fresh open: every pattern survives.
        let f = std::fs::File::open(tmp.path()).unwrap();
        let mut r = Qcow2Reader::open(f).unwrap();
        for guest_idx in 0..300u64 {
            r.seek(SeekFrom::Start(guest_idx * cs)).unwrap();
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf).unwrap();
            let expect = [(guest_idx as u8).wrapping_add(1); 8];
            assert_eq!(
                buf, expect,
                "round-trip mismatch at guest cluster {guest_idx}"
            );
        }
    }

    #[test]
    fn editor_reuses_free_clusters_before_appending() {
        // Build an image where the first guest cluster is allocated and the
        // second is sparse. Manually clear the data cluster's refcount entry
        // to simulate a free host cluster, then drive an edit that needs a
        // new data cluster. The editor must reuse the freed slot rather than
        // extend host_end.
        let cs = 512u64;
        let disk = 2 * cs;
        let mut backing = fresh_qcow2(disk, 9, &[0xCD; 16]).into_inner();

        // Locate the data cluster the writer allocated: walk header → L1 →
        // L2[0]. Header at byte 0, L1 at cluster_size (refcount_table at the
        // cluster after L1 region, etc.).
        let l1_off = cs;
        let l1_entry = u64::from_be_bytes(
            backing[l1_off as usize..l1_off as usize + 8]
                .try_into()
                .unwrap(),
        );
        let l2_host = l1_entry & QCOW2_OFFSET_MASK;
        let l2_entry = u64::from_be_bytes(
            backing[l2_host as usize..l2_host as usize + 8]
                .try_into()
                .unwrap(),
        );
        let data_host = l2_entry & QCOW2_OFFSET_MASK;
        // Free that cluster: zero its refcount entry + zero L2[0] so the
        // reader sees the slot as unallocated.
        let refcounts_per_block = cs / 2;
        let cluster_idx = data_host / cs;
        let table_idx = cluster_idx / refcounts_per_block;
        let entry_idx = cluster_idx % refcounts_per_block;
        // refcount_table_offset = l1_offset + l1_bytes_padded; the writer uses
        // 1 cluster for l1 region in this size, so rt is at 2*cs.
        let rt_off = 2 * cs;
        let rb_off = u64::from_be_bytes(
            backing[(rt_off + table_idx * 8) as usize..(rt_off + table_idx * 8) as usize + 8]
                .try_into()
                .unwrap(),
        );
        let rc_entry_off = (rb_off + entry_idx * 2) as usize;
        backing[rc_entry_off..rc_entry_off + 2].copy_from_slice(&0u16.to_be_bytes());
        backing[l2_host as usize..l2_host as usize + 8].copy_from_slice(&0u64.to_be_bytes());

        // Now open editable and write into the second guest cluster. The
        // allocator should hand back `data_host` (the freed slot) rather than
        // extending the file.
        let mut e = Qcow2Reader::open(Cursor::new(backing)).unwrap();
        let host_end_before = e.host_end;
        e.seek(SeekFrom::Start(cs)).unwrap();
        let payload = [0xEE; 16];
        e.write_all(&payload).unwrap();
        assert_eq!(
            e.host_end, host_end_before,
            "free-cluster reuse must not extend host_end"
        );

        // Round-trip via fresh reader.
        let bytes = e.inner.into_inner();
        let mut r = Qcow2Reader::open(Cursor::new(bytes)).unwrap();
        r.seek(SeekFrom::Start(cs)).unwrap();
        let mut buf = [0u8; 16];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(buf, payload);
    }

    /// **Phase 2 acceptance test** (docs/virtualization-formats.md, session 2.4).
    ///
    /// Round-trip a UTM-shape APM + classic-HFS disk through every QCOW2
    /// surface: build the source in memory → `export_qcow2` → reopen as a
    /// `Qcow2Reader<File>` → host an `EditableFilesystem` over the inner HFS
    /// partition → create a file via the HFS editor → close → `qemu-img
    /// check` → reopen read-only → confirm the new file is visible with the
    /// expected payload. This exercises the writer, the in-place editor
    /// (allocate-on-write into clusters the writer left unmapped), and the
    /// `Send + 'static` host requirement of `open_editable_filesystem` all in
    /// one path.
    #[test]
    fn acceptance_apm_hfs_round_trip_through_qcow2_edit() {
        use crate::fs::filesystem::CreateFileOptions;
        use crate::fs::hfs::create_blank_hfs;
        use crate::fs::hfs_clone::emit_apm_disk_with_hfs;
        use crate::partition::apm::build_minimal_apm;

        // 1. Build a small APM disk with one Apple_HFS partition. 4096
        //    sectors × 512 B = 2 MiB. Apple_HFS at sector 96 (= 0xC000).
        let total_blocks: u32 = 4096;
        let hfs_start: u32 = 96;
        let mut apm = build_minimal_apm(
            &[
                ("Apple_Driver43".into(), 32, 8),
                ("Apple_HFS".into(), hfs_start, total_blocks - hfs_start),
            ],
            512,
            total_blocks,
        );
        apm.entries[2].name = "MacOS".into();
        let apm_blocks = apm.build_apm_blocks(Some(total_blocks));
        let mut source = vec![0u8; total_blocks as usize * 512];
        source[..apm_blocks.len()].copy_from_slice(&apm_blocks);
        let hfs = create_blank_hfs((total_blocks - hfs_start) as u64 * 512, 4096, "Acceptance")
            .expect("create_blank_hfs");
        let mut raw_disk: Vec<u8> = Vec::new();
        let src_len = source.len() as u64;
        emit_apm_disk_with_hfs(
            &mut Cursor::new(source),
            src_len,
            &hfs,
            &mut Cursor::new(&mut raw_disk),
        )
        .expect("emit_apm_disk_with_hfs");
        let disk_size = raw_disk.len() as u64;
        let hfs_partition_offset = (hfs_start as u64) * 512;

        // 2. Export the raw disk to a QCOW2 tempfile via the writer.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            export_qcow2(
                &mut Cursor::new(&raw_disk),
                &mut f,
                disk_size,
                0, // default 64 KiB clusters
                &mut |_| {},
                &|| false,
            )
            .expect("export_qcow2");
        }

        // 3. Open the QCOW2 read-write as a `Qcow2Reader<File>` and host the
        //    Apple_HFS partition through `open_editable_filesystem`. This is
        //    the same shape the GUI / CLI would use once browse-view edit
        //    learns container readers — Session 2.4 ships the engine path;
        //    the browse-view integration is an incremental follow-up.
        let payload = b"hello from qcow2 in-place edit acceptance test\n";
        let file_name = "GREETING.TXT";
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            let reader = Qcow2Reader::open(f).expect("Qcow2Reader::open r+w");
            let mut efs = crate::fs::open_editable_filesystem(
                reader,
                hfs_partition_offset,
                0, // partition_type byte ignored for APM (type_string drives)
                Some("Apple_HFS"),
            )
            .expect("open_editable_filesystem over Qcow2Reader");
            let root = efs.root().expect("root");
            let opts = CreateFileOptions::default();
            let mut data_reader = std::io::Cursor::new(payload);
            efs.create_file(
                &root,
                file_name,
                &mut data_reader,
                payload.len() as u64,
                &opts,
            )
            .expect("create_file");
            efs.sync_metadata().expect("sync_metadata");
        }

        // 4. External validator: qemu-img check must be happy with the edited
        //    image (refcounts consistent, no truncation). Gated like all the
        //    other qemu-img tests — skip cleanly when qemu-img isn't present.
        match crate::rbformats::qemu_img_test::qemu_img_check(tmp.path()) {
            Ok(true) => {}
            Ok(false) => eprintln!("skipping qemu-img check — `qemu-img` not on PATH"),
            Err(e) => panic!("qemu-img check failed on edited APM+HFS image: {e}"),
        }

        // 5. Reopen the QCOW2 read-only, walk the HFS catalog, find the file.
        let f = std::fs::File::open(tmp.path()).unwrap();
        let reader = Qcow2Reader::open(f).unwrap();
        let mut fs = crate::fs::open_filesystem(reader, hfs_partition_offset, 0, Some("Apple_HFS"))
            .expect("open_filesystem after edit");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries
            .iter()
            .find(|e| e.name == file_name)
            .unwrap_or_else(|| panic!("file {file_name} not visible after round-trip"));
        let buf = fs.read_file(entry, payload.len() * 2).unwrap();
        assert_eq!(buf, payload, "payload mismatch after QCOW2 edit + reopen");
    }

    #[test]
    fn editor_output_passes_qemu_img_check() {
        // Edit a fresh image, then run qemu-img check on the result. Gated:
        // skipped when qemu-img isn't installed. Covers the realistic flow:
        // open → grow L2 + allocate data → close → external validator says OK.
        let disk = 64 * 1024u64;
        let backing = fresh_qcow2(disk, 9, &[0x55; 16]);
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), backing.get_ref()).unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            let mut e = Qcow2Reader::open(f).unwrap();
            // Write into a guest cluster that needs a fresh L2 (slot 1).
            e.seek(SeekFrom::Start(64 * 512)).unwrap();
            e.write_all(&[0xDE; 256]).unwrap();
            e.flush().unwrap();
        }
        match crate::rbformats::qemu_img_test::qemu_img_check(tmp.path()) {
            Ok(true) => {}
            Ok(false) => eprintln!("skipping qemu-img check — `qemu-img` not on PATH"),
            Err(e) => panic!("qemu-img check failed on edited image: {e}"),
        }
    }

    /// `qemu-img check` smoke test against a writer-produced image. Skipped
    /// (printed) when `qemu-img` isn't on PATH. Session 4.2 (VMDK sparse) will
    /// reuse [`crate::rbformats::qemu_img_test::qemu_img_check`].
    #[test]
    fn writer_round_trips_through_qemu_img_check() {
        let disk_size = 64 * 1024u64;
        let src = make_pattern(disk_size as usize);
        let tmp = tempfile::NamedTempFile::new().unwrap();
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            export_qcow2(
                &mut Cursor::new(&src),
                &mut f,
                disk_size,
                0,
                &mut |_| {},
                &|| false,
            )
            .unwrap();
        }
        match crate::rbformats::qemu_img_test::qemu_img_check(tmp.path()) {
            Ok(true) => {} // passed
            Ok(false) => eprintln!("skipping qemu-img check — `qemu-img` not on PATH"),
            Err(e) => panic!("qemu-img check failed on writer output: {e}"),
        }
        // Independent round-trip via our own reader.
        let f = std::fs::File::open(tmp.path()).unwrap();
        assert_qcow2_round_trips(Cursor::new(std::fs::read(tmp.path()).unwrap()), &src);
        drop(f);
    }
}
