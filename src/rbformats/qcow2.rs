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

use std::io::{Read, Seek, SeekFrom};

use anyhow::{bail, Result};

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
    fn rejects_encryption() {
        let cluster_bits = 9;
        let mut img = build_minimal_qcow2(3, cluster_bits, 1u64 << cluster_bits, &[], &[]);
        img[32..36].copy_from_slice(&1u32.to_be_bytes());
        let err = open_expecting_err(img);
        assert!(err.to_string().contains("encrypted"));
    }

    #[test]
    fn rejects_snapshots() {
        let cluster_bits = 9;
        let mut img = build_minimal_qcow2(3, cluster_bits, 1u64 << cluster_bits, &[], &[]);
        img[60..64].copy_from_slice(&3u32.to_be_bytes());
        let err = open_expecting_err(img);
        assert!(err.to_string().contains("snapshot"));
    }

    #[test]
    fn rejects_unknown_incompat_bit() {
        let cluster_bits = 9;
        let mut img = build_minimal_qcow2(3, cluster_bits, 1u64 << cluster_bits, &[], &[]);
        // Set bit 5 — outside the known mask, must abort.
        img[72..80].copy_from_slice(&(1u64 << 5).to_be_bytes());
        let err = open_expecting_err(img);
        assert!(err.to_string().contains("unknown incompatible_features"));
    }

    #[test]
    fn rejects_external_data_file() {
        let cluster_bits = 9;
        let mut img = build_minimal_qcow2(3, cluster_bits, 1u64 << cluster_bits, &[], &[]);
        img[72..80].copy_from_slice(&(1u64 << 2).to_be_bytes());
        let err = open_expecting_err(img);
        assert!(err.to_string().contains("external data file"));
    }

    #[test]
    fn rejects_extended_l2() {
        let cluster_bits = 9;
        let mut img = build_minimal_qcow2(3, cluster_bits, 1u64 << cluster_bits, &[], &[]);
        img[72..80].copy_from_slice(&(1u64 << 4).to_be_bytes());
        let err = open_expecting_err(img);
        assert!(err.to_string().contains("extended L2"));
    }

    #[test]
    fn rejects_backing_file() {
        let cluster_bits = 9;
        let mut img = build_minimal_qcow2(3, cluster_bits, 1u64 << cluster_bits, &[], &[]);
        img[8..16].copy_from_slice(&0xDEAD_BEEFu64.to_be_bytes());
        let err = open_expecting_err(img);
        assert!(err.to_string().contains("backing file"));
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
}
