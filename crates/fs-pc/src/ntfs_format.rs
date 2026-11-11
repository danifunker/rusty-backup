//! Clean-room, geometry-parametric NTFS formatter — the foundation for the
//! defragmenting NTFS clone, the peer of [`crate::exfat::create_blank_exfat`].
//!
//! Unlike the previous template-blob formatter (a single hardcoded
//! 512 B-sector / 4096 B-cluster / 1024 B-record shape lifted byte-for-byte from
//! a `mkntfs` image), this synthesises the **geometry-dependent backbone** — the
//! boot sector / BPB, the 16 system `FILE` records, their runlists, the
//! update-sequence (fixup) arrays, the cluster and MFT bitmaps, `$MFTMirr` and
//! the root directory index — parametrically from a caller-chosen
//! [`NtfsGeometry`] (variable sector size *and* variable cluster size).
//!
//! Only the genuinely **geometry-independent data tables** (`$AttrDef`,
//! `$UpCase`, the `$Secure:$SDS` stream, the standard security descriptors and a
//! handful of small resident attribute payloads) are reused verbatim; they live
//! in [`crate::ntfs_tables`]. See `docs/ntfs_format_cleanroom_notes.md` for
//! the clean-room provenance, the supported geometry and the cluster-size
//! defaults. No `mkntfs.c` / `ntfs-3g` source (GPL-2) was read or translated.

#[cfg(feature = "rust173-polyfill")]
use disk_core::rust173::IntIsMultipleOf as _;
use std::io::{Seek, SeekFrom, Write};

use crate::ntfs_tables::{
    ATTRDEF, NT4_ATTRDEF, NT4_SD_METAFILE, NT4_SD_METAFILE_RW, NT4_SD_ROOT, NT4_UPCASE,
    ROOT_SECDESC, SECURE_SDH_IDXROOT, SECURE_SDS, SECURE_SII_IDXROOT, SEC_DESC_100, UPCASE,
    UPCASE_INFO,
};
use disk_core::filesystem::FilesystemError;

// ---- Constants (data, geometry-independent) ----

/// A valid NTFS timestamp (100 ns ticks since 1601), used for every system
/// file's create/modify/access/MFT-change times. Observed from `mkntfs` output;
/// the value is cosmetic (the volume is fresh) and geometry-independent.
const TS: u64 = 0x01DD_02AC_FD67_0300;

/// Parent reference (MFT record 5, sequence 5) of the root directory. Every
/// system file's `$FILE_NAME` names the root as its parent.
const ROOT_PARENT_REF: u64 = 5 | (5u64 << 48);

/// Standard "first" security id assigned in `$STANDARD_INFORMATION` for the
/// metafiles that reference the centralised `$Secure` stream rather than
/// carrying their own `$SECURITY_DESCRIPTOR`.
const SECURITY_ID: u32 = 0x100;

// NTFS file-attribute flags used in `$STANDARD_INFORMATION` / `$FILE_NAME`.
const FA_HIDDEN: u32 = 0x02;
const FA_SYSTEM: u32 = 0x04;
const FA_DIR_INDEX: u32 = 0x1000_0000; // "$I30 index present" -> directory

/// NTFS fixup (update-sequence) block size. Multi-sector structures (`FILE` and
/// `INDX` records) carry one update-sequence entry per **512-byte block**,
/// regardless of the volume's actual sector size — verified against `mkntfs`
/// output (a 4096-byte record on a 4096-byte-sector volume still uses 9 USA
/// entries = 4096/512 + 1) and required by `ntfs-3g`.
const NTFS_BLOCK_SIZE: usize = 512;

// ---- little-endian helpers ----

#[inline]
fn cdiv(a: u64, b: u64) -> u64 {
    a.div_ceil(b)
}
#[inline]
fn wu16(b: &mut [u8], o: usize, v: u16) {
    b[o..o + 2].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn wu32(b: &mut [u8], o: usize, v: u32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn wu64(b: &mut [u8], o: usize, v: u64) {
    b[o..o + 8].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn align8(n: usize) -> usize {
    (n + 7) & !7
}

// =====================================================================
//  Geometry
// =====================================================================

/// On-disk geometry of an NTFS volume. `cluster_size = bytes_per_sector *
/// sectors_per_cluster`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NtfsGeometry {
    /// 512, 1024, 2048 or 4096.
    pub bytes_per_sector: u32,
    /// Power of two; `cluster_size = bytes_per_sector * sectors_per_cluster`.
    pub sectors_per_cluster: u32,
    /// MFT record size in bytes (default `max(1024, bytes_per_sector)`).
    pub mft_record_size: u32,
    /// Index record size in bytes (default `max(4096, bytes_per_sector)`).
    pub index_record_size: u32,
}

impl NtfsGeometry {
    /// Cluster size in bytes.
    pub fn cluster_size(&self) -> u64 {
        self.bytes_per_sector as u64 * self.sectors_per_cluster as u64
    }

    fn default_mft_record_size(sector: u32) -> u32 {
        sector.max(1024)
    }
    fn default_index_record_size(sector: u32) -> u32 {
        sector.max(4096)
    }

    /// Explicit, validated geometry from a cluster size and sector size (both in
    /// bytes). MFT- and index-record sizes take their geometry-derived defaults.
    pub fn with_cluster_size(
        cluster_size: u32,
        bytes_per_sector: u32,
    ) -> Result<Self, FilesystemError> {
        if !matches!(bytes_per_sector, 512 | 1024 | 2048 | 4096) {
            return Err(FilesystemError::InvalidData(format!(
                "NTFS geometry: bytes_per_sector {bytes_per_sector} must be 512, 1024, 2048 or 4096"
            )));
        }
        if cluster_size < bytes_per_sector
            || !cluster_size.is_power_of_two()
            || !cluster_size.is_multiple_of(bytes_per_sector)
        {
            return Err(FilesystemError::InvalidData(format!(
                "NTFS geometry: cluster size {cluster_size} must be a power-of-two multiple of the {bytes_per_sector}-byte sector"
            )));
        }
        if cluster_size as u64 > 2 * 1024 * 1024 {
            return Err(FilesystemError::InvalidData(format!(
                "NTFS geometry: cluster size {cluster_size} exceeds the 2 MiB NTFS maximum"
            )));
        }
        let g = Self {
            bytes_per_sector,
            sectors_per_cluster: cluster_size / bytes_per_sector,
            mft_record_size: Self::default_mft_record_size(bytes_per_sector),
            index_record_size: Self::default_index_record_size(bytes_per_sector),
        };
        g.validate()?;
        Ok(g)
    }

    /// Default geometry for a volume of `total_size` bytes, replicating the
    /// classic `mkntfs` default-cluster-size-by-size table (512-byte sectors).
    /// See `docs/ntfs_format_cleanroom_notes.md`.
    pub fn for_volume_size(total_size: u64) -> Self {
        const MIB: u64 = 1024 * 1024;
        let cluster = if total_size <= 512 * MIB {
            512
        } else if total_size <= 1024 * MIB {
            1024
        } else if total_size <= 2048 * MIB {
            2048
        } else {
            4096
        };
        // 512-byte sector with a power-of-two cluster is always valid.
        Self::with_cluster_size(cluster, 512).expect("default geometry is valid")
    }

    /// Validate the geometry; returns [`FilesystemError::InvalidData`] on any
    /// violation of the documented NTFS limits.
    pub fn validate(&self) -> Result<(), FilesystemError> {
        let bps = self.bytes_per_sector;
        if !matches!(bps, 512 | 1024 | 2048 | 4096) {
            return Err(FilesystemError::InvalidData(format!(
                "NTFS geometry: bytes_per_sector {bps} invalid"
            )));
        }
        if !self.sectors_per_cluster.is_power_of_two() {
            return Err(FilesystemError::InvalidData(format!(
                "NTFS geometry: sectors_per_cluster {} is not a power of two",
                self.sectors_per_cluster
            )));
        }
        let cluster = self.cluster_size();
        if cluster < bps as u64 || cluster > 2 * 1024 * 1024 {
            return Err(FilesystemError::InvalidData(format!(
                "NTFS geometry: cluster size {cluster} out of range [{bps}, 2 MiB]"
            )));
        }
        for (name, size) in [
            ("mft_record_size", self.mft_record_size),
            ("index_record_size", self.index_record_size),
        ] {
            if !size.is_power_of_two() || size % bps != 0 || size < bps {
                return Err(FilesystemError::InvalidData(format!(
                    "NTFS geometry: {name} {size} must be a power-of-two multiple of the {bps}-byte sector"
                )));
            }
        }
        Ok(())
    }

    /// `sectors_per_cluster` encoded for BPB offset 0x0D: a plain unsigned byte
    /// up to 128, else the signed negative power `-log2(spc)` (for clusters
    /// where `spc >= 256`, e.g. 128 KiB clusters on 512-byte sectors).
    fn spc_byte(&self) -> u8 {
        let spc = self.sectors_per_cluster;
        if spc <= 128 {
            spc as u8
        } else {
            (-(spc.trailing_zeros() as i32) as i8) as u8
        }
    }

    /// BPB offset 0x40 / 0x44 signed encoding: a positive cluster count when the
    /// structure is at least one cluster, else the signed `-log2(size)`.
    fn signed_power_byte(structure: u32, cluster: u64) -> u8 {
        if structure as u64 >= cluster {
            (structure as u64 / cluster) as u8
        } else {
            (-(structure.trailing_zeros() as i32) as i8) as u8
        }
    }

    fn clusters_per_mft_byte(&self) -> u8 {
        Self::signed_power_byte(self.mft_record_size, self.cluster_size())
    }
    fn clusters_per_index_byte(&self) -> u8 {
        Self::signed_power_byte(self.index_record_size, self.cluster_size())
    }

    /// The `clusters_per_index_block` byte inside an `$INDEX_ROOT` header. Unlike
    /// BPB 0x44 this is *not* a negative-power encoding: when the index record is
    /// smaller than a cluster `mkntfs` stores `index_record_size / sector`.
    fn idxroot_clusters_per_block(&self) -> u8 {
        let cluster = self.cluster_size();
        if self.index_record_size as u64 >= cluster {
            (self.index_record_size as u64 / cluster) as u8
        } else {
            (self.index_record_size / self.bytes_per_sector) as u8
        }
    }

    fn usa_count(&self, structure_size: u32) -> u16 {
        (structure_size / NTFS_BLOCK_SIZE as u32 + 1) as u16
    }
}

/// The on-disk NTFS version a volume is formatted as. Windows NT 3.5 / 3.51 / 4.0 (before SP4)
/// mount only major version 1; Windows 2000 and later read both and upgrade 1.2 on first mount.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NtfsVersion {
    /// NTFS 1.2: Windows NT 3.5, 3.51 and 4.0.
    V1_2,
    /// NTFS 3.1: Windows XP and later (what Windows 2000 SP and up write).
    #[default]
    V3_1,
}

impl NtfsVersion {
    /// `$VOLUME_INFORMATION` major and minor version.
    pub fn major_minor(self) -> (u8, u8) {
        match self {
            NtfsVersion::V1_2 => (1, 2),
            NtfsVersion::V3_1 => (3, 1),
        }
    }

    /// The version a volume reports (`major.minor`), when this formatter can write it.
    pub fn from_major_minor(major: u8, minor: u8) -> Option<Self> {
        match (major, minor) {
            (1, _) => Some(NtfsVersion::V1_2),
            (3, _) => Some(NtfsVersion::V3_1),
            _ => None,
        }
    }

    /// "1.2" / "3.1".
    pub fn label(self) -> &'static str {
        match self {
            NtfsVersion::V1_2 => "1.2",
            NtfsVersion::V3_1 => "3.1",
        }
    }

    /// The Windows versions that mount it, for help text and prompts.
    pub fn readers(self) -> &'static str {
        match self {
            NtfsVersion::V1_2 => "Windows NT 3.5, 3.51 and 4.0, and every later Windows",
            NtfsVersion::V3_1 => "Windows XP and later (and NT 4.0 SP4 or later, read-write)",
        }
    }

    /// Why NT can't use `geo` for this version, if it can't: 1.2 is 512-byte sectors only, and
    /// clusters over 4 KiB lose NT 4.0's compression.
    pub fn geometry_problem(self, geo: &NtfsGeometry) -> Option<String> {
        if self != NtfsVersion::V1_2 {
            return None;
        }
        if geo.bytes_per_sector != 512 {
            return Some(format!(
                "NTFS 1.2 uses 512-byte sectors; {} was asked for",
                geo.bytes_per_sector
            ));
        }
        if geo.cluster_size() > 4096 {
            return Some(format!(
                "NTFS 1.2 clusters over 4 KiB ({} B) lose NT 4.0's compression",
                geo.cluster_size()
            ));
        }
        None
    }

    /// Where a `FILE` record's update sequence array starts: 1.2 has no record-number field.
    fn usa_offset(self) -> u16 {
        match self {
            NtfsVersion::V1_2 => 0x2A,
            NtfsVersion::V3_1 => 0x30,
        }
    }

    /// Records the formatter marks in use in the MFT bitmap (NT 4.0 puts user files at 16).
    fn reserved_records(self) -> u64 {
        match self {
            NtfsVersion::V1_2 => 16,
            NtfsVersion::V3_1 => 24,
        }
    }
}

/// Parameters for [`create_ntfs`].
pub struct NtfsFormatParams {
    /// Total volume size in bytes; must be a multiple of the cluster size.
    pub total_size: u64,
    /// On-disk geometry.
    pub geometry: NtfsGeometry,
    /// Lower bound on MFT record capacity (records, including the 24 reserved).
    pub mft_records_hint: u64,
    /// Optional volume label.
    pub label: Option<String>,
}

// =====================================================================
//  Runlist (mapping pairs) encoding
// =====================================================================

/// Encode a single data run. `lcn == None` -> a sparse run (length only).
///
/// Both the length and the LCN offset are encoded "sign-safe": an extra byte is
/// reserved whenever the value's most-significant bit would land in the top
/// byte, so the field never reads back as negative. NTFS treats a run length
/// with its high bit set as invalid (e.g. a 128-cluster run must be `12 80 00`,
/// not `11 80`).
fn enc_run(length: u64, lcn: Option<u64>) -> Vec<u8> {
    let lbits = 64 - length.leading_zeros();
    let lb = (((lbits + 8) / 8).max(1)) as usize;
    match lcn {
        None => {
            let mut v = vec![lb as u8];
            v.extend_from_slice(&length.to_le_bytes()[..lb]);
            v
        }
        Some(l) => {
            // Signed LCN offset; reserve a byte so the sign bit stays clear.
            let bits = 64 - l.leading_zeros();
            let ob = (((bits + 8) / 8).max(1)) as usize;
            let mut v = vec![(lb | (ob << 4)) as u8];
            v.extend_from_slice(&length.to_le_bytes()[..lb]);
            v.extend_from_slice(&(l as i64).to_le_bytes()[..ob]);
            v
        }
    }
}

/// Terminated single-run mapping pairs (run + 0x00 terminator).
fn runlist(length: u64, lcn: Option<u64>) -> Vec<u8> {
    let mut r = enc_run(length, lcn);
    r.push(0);
    r
}

// =====================================================================
//  Update sequence (fixup) array
// =====================================================================

/// Bump the USN and stash the last two bytes of every 512-byte NTFS block into
/// the update sequence array, replacing them with the USN.
fn fixup_write(rec: &mut [u8]) {
    let uoff = u16::from_le_bytes([rec[4], rec[5]]) as usize;
    let ucnt = u16::from_le_bytes([rec[6], rec[7]]) as usize;
    let mut usn = u16::from_le_bytes([rec[uoff], rec[uoff + 1]]).wrapping_add(1);
    if usn == 0 {
        usn = 1;
    }
    wu16(rec, uoff, usn);
    for i in 1..ucnt {
        let tail = i * NTFS_BLOCK_SIZE - 2;
        let last = u16::from_le_bytes([rec[tail], rec[tail + 1]]);
        wu16(rec, uoff + i * 2, last);
        wu16(rec, tail, usn);
    }
}

// =====================================================================
//  Attribute builders (each returns an 8-byte-aligned attribute record)
// =====================================================================

/// Build a resident attribute. `indexed` sets the resident flag 0x01 used by
/// `$FILE_NAME` (it is also indexed in the parent's `$I30`).
fn resident_attr(atype: u32, name: &str, value: &[u8], instance: u16, indexed: bool) -> Vec<u8> {
    let nu: Vec<u16> = name.encode_utf16().collect();
    let nlen = nu.len();
    let name_off = 0x18usize;
    let val_off = if nlen > 0 {
        align8(name_off + nlen * 2)
    } else {
        name_off
    };
    let total = align8(val_off + value.len());
    let mut a = vec![0u8; total];
    wu32(&mut a, 0, atype);
    wu32(&mut a, 4, total as u32);
    a[8] = 0; // resident
    a[9] = nlen as u8;
    wu16(&mut a, 0x0A, name_off as u16);
    wu16(&mut a, 0x0C, 0); // flags
    wu16(&mut a, 0x0E, instance);
    wu32(&mut a, 0x10, value.len() as u32);
    wu16(&mut a, 0x14, val_off as u16);
    a[0x16] = if indexed { 1 } else { 0 };
    for (i, u) in nu.iter().enumerate() {
        wu16(&mut a, name_off + i * 2, *u);
    }
    a[val_off..val_off + value.len()].copy_from_slice(value);
    a
}

/// Build a non-resident attribute with a single (already-terminated) runlist.
#[allow(clippy::too_many_arguments)]
fn nonresident_attr(
    atype: u32,
    name: &str,
    run: &[u8],
    last_vcn: u64,
    alloc: u64,
    real: u64,
    instance: u16,
    attr_flags: u16,
) -> Vec<u8> {
    let nu: Vec<u16> = name.encode_utf16().collect();
    let nlen = nu.len();
    let name_off = 0x40usize;
    let mp_off = if nlen > 0 {
        align8(name_off + nlen * 2)
    } else {
        name_off
    };
    let total = align8(mp_off + run.len());
    let mut a = vec![0u8; total];
    wu32(&mut a, 0, atype);
    wu32(&mut a, 4, total as u32);
    a[8] = 1; // non-resident
    a[9] = nlen as u8;
    wu16(&mut a, 0x0A, name_off as u16);
    wu16(&mut a, 0x0C, attr_flags);
    wu16(&mut a, 0x0E, instance);
    wu64(&mut a, 0x10, 0); // start VCN
    wu64(&mut a, 0x18, last_vcn);
    wu16(&mut a, 0x20, mp_off as u16);
    wu16(&mut a, 0x22, 0); // compression unit
    wu64(&mut a, 0x28, alloc);
    wu64(&mut a, 0x30, real);
    wu64(&mut a, 0x38, real); // initialized size
    for (i, u) in nu.iter().enumerate() {
        wu16(&mut a, name_off + i * 2, *u);
    }
    a[mp_off..mp_off + run.len()].copy_from_slice(run);
    a
}

/// `$STANDARD_INFORMATION` value (72-byte NTFS 3.x form).
fn std_info(file_attrs: u32, security_id: u32) -> Vec<u8> {
    let mut v = vec![0u8; 72];
    for i in 0..4 {
        wu64(&mut v, i * 8, TS);
    }
    wu32(&mut v, 0x20, file_attrs);
    wu32(&mut v, 0x34, security_id);
    v
}

/// `$STANDARD_INFORMATION` value (48-byte NTFS 1.2 form: times and attributes, no security id).
fn std_info_v12(file_attrs: u32) -> Vec<u8> {
    let mut v = vec![0u8; 48];
    for i in 0..4 {
        wu64(&mut v, i * 8, TS);
    }
    wu32(&mut v, 0x20, file_attrs);
    v
}

/// [`file_name_value`] with the allocated and real sizes filled in, as NT 4.0 writes them.
fn file_name_value_sized(name: &str, file_attrs: u32, alloc: u64, real: u64) -> Vec<u8> {
    let mut v = file_name_value(name, file_attrs);
    wu64(&mut v, 0x28, alloc);
    wu64(&mut v, 0x30, real);
    v
}

/// `$FILE_NAME` value naming `name` (parent = root, namespace WIN32_AND_DOS).
fn file_name_value(name: &str, file_attrs: u32) -> Vec<u8> {
    let nu: Vec<u16> = name.encode_utf16().collect();
    let mut v = vec![0u8; 66 + nu.len() * 2];
    wu64(&mut v, 0, ROOT_PARENT_REF);
    for i in 0..4 {
        wu64(&mut v, 8 + i * 8, TS);
    }
    // allocated_size (0x28) and real_size (0x30) left 0 — ntfs-3g uses the
    // $DATA sizes; leaving these 0 keeps the value geometry-independent.
    wu32(&mut v, 0x38, file_attrs);
    v[0x40] = nu.len() as u8;
    v[0x41] = 3; // WIN32_AND_DOS namespace
    for (i, u) in nu.iter().enumerate() {
        wu16(&mut v, 0x42 + i * 2, *u);
    }
    v
}

/// `$VOLUME_NAME` value (label as UTF-16LE).
fn volume_name_value(label: &str) -> Vec<u8> {
    let mut v = Vec::new();
    for u in label.encode_utf16() {
        v.extend_from_slice(&u.to_le_bytes());
    }
    v
}

/// `$VOLUME_INFORMATION` value: the version, flags 0 (clean).
fn volume_info_value(version: NtfsVersion) -> Vec<u8> {
    let mut v = vec![0u8; 12];
    (v[8], v[9]) = version.major_minor();
    v
}

/// Header (32-byte) shared by `$INDEX_ROOT` index-node variants.
fn index_root_header(geo: &NtfsGeometry, node_flags: u32, used_after_hdr: u32) -> Vec<u8> {
    let mut v = vec![0u8; 32];
    wu32(&mut v, 0, 0x30); // indexed attribute type = $FILE_NAME
    wu32(&mut v, 4, 1); // COLLATION_FILE_NAME
    wu32(&mut v, 8, geo.index_record_size);
    v[12] = geo.idxroot_clusters_per_block();
    // index node header (relative to offset 16):
    wu32(&mut v, 16, 16); // entries offset
    wu32(&mut v, 20, used_after_hdr); // index_used
    wu32(&mut v, 24, used_after_hdr); // index_allocated (resident: == used)
    wu32(&mut v, 28, node_flags);
    v
}

/// Root directory `$INDEX_ROOT` value: a "large" index whose single entry is the
/// end marker pointing at the `$INDEX_ALLOCATION` INDX block at VCN 0.
fn root_index_root(geo: &NtfsGeometry) -> Vec<u8> {
    let mut v = index_root_header(geo, 1 /* LARGE */, 40 /* 16 hdr + 24 end entry */);
    v.resize(56, 0);
    // end entry (24 bytes) at offset 32:
    wu16(&mut v, 32 + 8, 24); // entry length
    wu16(&mut v, 32 + 12, 3); // flags: LAST | has sub-node
    wu64(&mut v, 32 + 16, 0); // sub-node VCN
    v
}

/// Empty directory `$INDEX_ROOT` value ($Extend): a "small" index with only the
/// end marker.
fn empty_index_root(geo: &NtfsGeometry) -> Vec<u8> {
    let mut v = index_root_header(geo, 0 /* SMALL */, 32 /* 16 hdr + 16 end entry */);
    v.resize(48, 0);
    // end entry (16 bytes) at offset 32:
    wu16(&mut v, 32 + 8, 16); // entry length
    wu16(&mut v, 32 + 12, 2); // flags: LAST
    v
}

/// A `$FILE_NAME` index entry for the root index (key = `$FILE_NAME` value).
fn index_entry(mft_ref: u64, key: &[u8]) -> Vec<u8> {
    let entry_len = align8(16 + key.len());
    let mut e = vec![0u8; entry_len];
    wu64(&mut e, 0, mft_ref);
    wu16(&mut e, 8, entry_len as u16);
    wu16(&mut e, 10, key.len() as u16);
    wu16(&mut e, 12, 0); // flags
    e[16..16 + key.len()].copy_from_slice(key);
    e
}

// =====================================================================
//  FILE record assembly
// =====================================================================

fn mft_ref(rec_no: u64) -> u64 {
    let seq = rec_no.max(1);
    rec_no | (seq << 48)
}

/// Assemble an in-use system `FILE` record from its attributes.
fn build_record(rec_no: u64, mft_flags: u16, geo: &NtfsGeometry, attrs: &[Vec<u8>]) -> Vec<u8> {
    build_record_as(NtfsVersion::V3_1, rec_no, mft_flags, 1, geo, attrs)
}

/// [`build_record`] for `version`, with its hard-link count (0 for NT's reserved records).
fn build_record_as(
    version: NtfsVersion,
    rec_no: u64,
    mft_flags: u16,
    links: u16,
    geo: &NtfsGeometry,
    attrs: &[Vec<u8>],
) -> Vec<u8> {
    let rs = geo.mft_record_size as usize;
    let mut r = vec![0u8; rs];
    r[0..4].copy_from_slice(b"FILE");
    let usa_off = version.usa_offset();
    let usa_count = geo.usa_count(geo.mft_record_size);
    wu16(&mut r, 4, usa_off);
    wu16(&mut r, 6, usa_count);
    wu16(&mut r, 0x10, rec_no.max(1) as u16); // sequence number
    wu16(&mut r, 0x12, links);
    let attrs_off = align8(usa_off as usize + usa_count as usize * 2);
    wu16(&mut r, 0x14, attrs_off as u16);
    wu16(&mut r, 0x16, mft_flags);
    wu32(&mut r, 0x1C, rs as u32); // allocated size
    wu16(&mut r, 0x28, attrs.len() as u16); // next attribute instance
    if version == NtfsVersion::V3_1 {
        wu32(&mut r, 0x2C, rec_no as u32); // MFT record number (3.x header only)
    }
    let mut o = attrs_off;
    for a in attrs {
        r[o..o + a.len()].copy_from_slice(a);
        o += a.len();
    }
    wu32(&mut r, o, 0xFFFF_FFFF); // attribute-list terminator
    o += 4;
    wu32(&mut r, 0x18, align8(o) as u32); // used size
    fixup_write(&mut r);
    r
}

/// A blank, unused (`sequence 0`) `FILE` record.
fn blank_record(version: NtfsVersion, rec_no: u64, geo: &NtfsGeometry) -> Vec<u8> {
    let rs = geo.mft_record_size as usize;
    let mut r = vec![0u8; rs];
    r[0..4].copy_from_slice(b"FILE");
    let usa_off = version.usa_offset();
    let usa_count = geo.usa_count(geo.mft_record_size);
    wu16(&mut r, 4, usa_off);
    wu16(&mut r, 6, usa_count);
    wu16(&mut r, 0x10, 0); // sequence 0 = unused
    let attrs_off = align8(usa_off as usize + usa_count as usize * 2);
    wu16(&mut r, 0x14, attrs_off as u16);
    wu16(&mut r, 0x16, 0); // not in use
    wu32(&mut r, 0x1C, rs as u32);
    if version == NtfsVersion::V3_1 {
        wu32(&mut r, 0x2C, rec_no as u32);
    }
    wu32(&mut r, attrs_off, 0xFFFF_FFFF);
    wu32(&mut r, 0x18, align8(attrs_off + 4) as u32);
    fixup_write(&mut r);
    r
}

/// Build the INDX block holding the root directory's index entries.
fn build_root_index_block(geo: &NtfsGeometry, entries: &[(u64, Vec<u8>)]) -> Vec<u8> {
    let isize = geo.index_record_size as usize;
    let mut b = vec![0u8; isize];
    b[0..4].copy_from_slice(b"INDX");
    let usa_off = 0x28u16;
    let usa_count = geo.usa_count(geo.index_record_size);
    wu16(&mut b, 4, usa_off);
    wu16(&mut b, 6, usa_count);
    wu64(&mut b, 0x10, 0); // VCN of this index block
    let node_hdr = 0x18usize;
    let entries_start = align8(usa_off as usize + usa_count as usize * 2);
    let mut o = entries_start;
    for (ref_, key) in entries {
        let e = index_entry(*ref_, key);
        b[o..o + e.len()].copy_from_slice(&e);
        o += e.len();
    }
    // End entry (16 bytes, flags LAST).
    wu64(&mut b, o, 0);
    wu16(&mut b, o + 8, 16);
    wu16(&mut b, o + 12, 2);
    o += 16;
    // Index node header (relative to node_hdr).
    wu32(&mut b, node_hdr, (entries_start - node_hdr) as u32); // entries offset
    wu32(&mut b, node_hdr + 4, (o - node_hdr) as u32); // index_used
    wu32(&mut b, node_hdr + 8, (isize - node_hdr) as u32); // index_allocated
    wu32(&mut b, node_hdr + 12, 0); // leaf node
    fixup_write(&mut b);
    b
}

// =====================================================================
//  Boot sector / BPB
// =====================================================================

fn build_boot(
    geo: &NtfsGeometry,
    total_sectors: u64,
    mft_lcn: u64,
    mftmirr_lcn: u64,
    serial: u64,
) -> Vec<u8> {
    let mut b = vec![0u8; 512];
    b[0] = 0xEB; // jmp
    b[1] = 0x52;
    b[2] = 0x90;
    b[3..11].copy_from_slice(b"NTFS    ");
    wu16(&mut b, 0x0B, geo.bytes_per_sector as u16);
    b[0x0D] = geo.spc_byte();
    b[0x15] = 0xF8; // media descriptor (fixed disk)
    b[0x24] = 0x80; // BIOS drive number
    b[0x26] = 0x80; // extended boot signature
    wu64(&mut b, 0x28, total_sectors);
    wu64(&mut b, 0x30, mft_lcn);
    wu64(&mut b, 0x38, mftmirr_lcn);
    b[0x40] = geo.clusters_per_mft_byte();
    b[0x44] = geo.clusters_per_index_byte();
    wu64(&mut b, 0x48, serial);
    // 0x50 checksum stays 0; boot code (0x54..0x1FD) stays zero (clean-room).
    b[0x1FE] = 0x55;
    b[0x1FF] = 0xAA;
    b
}

// =====================================================================
//  Sizing helpers
// =====================================================================

/// Smallest MFT-record count that holds the 24 reserved system records plus
/// `entries` user files/dirs, with headroom.
pub fn ntfs_mft_records_for(entries: u64) -> u64 {
    (24 + entries + entries / 8 + 32).max(64)
}

/// Smallest target size (bytes, cluster-aligned) for a defragmenting clone
/// holding `used_bytes` of file data plus `entries` files/dirs of metadata
/// overhead, at the target `cluster_size`. Mirrors
/// [`crate::exfat::exfat_min_packed_size`].
pub fn ntfs_min_packed_size(used_bytes: u64, entries: u64, cluster_size: u64) -> u64 {
    let cluster = cluster_size.max(512);
    let mft_records = ntfs_mft_records_for(entries);
    let mft_c = cdiv(mft_records * 1024, cluster);
    let upcase_c = cdiv(UPCASE.len() as u64, cluster);
    let sds_c = cdiv(SECURE_SDS.len() as u64, cluster);
    let log_c = cdiv(2 * 1024 * 1024, cluster);
    // boot + mft-bitmap + mft + mftmirr + logfile + clusterbitmap + attrdef +
    // upcase + sds + root sd + root index + the data itself + margin.
    let data_c = cdiv(used_bytes, cluster);
    let meta_c = 2 + 1 + mft_c + 1 + log_c + 8 + 1 + upcase_c + sds_c + 2 + 1;
    let total_c = meta_c + data_c + data_c / 20 + 64;
    total_c * cluster
}

/// `$LogFile` size for `total_size`: scaled with the volume, floor 256 KiB, cap
/// 64 MiB, rounded up to a cluster. (ntfs-3g resets the log on first mount, so
/// the exact size is not critical — only that it is valid and fits.)
fn logfile_size(total_size: u64, cluster: u64) -> u64 {
    let raw = (total_size / 200).clamp(256 * 1024, 64 * 1024 * 1024);
    cdiv(raw, cluster) * cluster
}

// =====================================================================
//  Main entry point
// =====================================================================

/// Format a fresh, mountable blank NTFS volume of `params.total_size` bytes into
/// `target`, with the given [`NtfsGeometry`], MFT capacity and volume label.
pub fn create_ntfs<W: Write + Seek>(
    target: &mut W,
    params: &NtfsFormatParams,
) -> Result<(), FilesystemError> {
    create_ntfs_version(target, params, NtfsVersion::V3_1)
}

/// Where each metadata structure sits; shared by both versions' record builders.
#[derive(Clone, Copy)]
struct Layout {
    cluster: u64,
    tc: u64,
    isize: u64,
    boot_c: u64,
    mftbm_c: u64,
    mftbm_lcn: u64,
    mftbm_real: u64,
    mft_c: u64,
    mft_lcn: u64,
    mft_real: u64,
    mftmirr_c: u64,
    mftmirr_lcn: u64,
    mftmirr_real: u64,
    log_c: u64,
    log_lcn: u64,
    log_size: u64,
    bm_c: u64,
    bm_lcn: u64,
    clusterbm_real: u64,
    attrdef_c: u64,
    attrdef_lcn: u64,
    upcase_c: u64,
    upcase_lcn: u64,
    sds_c: u64,
    sds_lcn: u64,
    rootsd_c: u64,
    rootsd_lcn: u64,
    rootidx_c: u64,
    rootidx_lcn: u64,
}

/// The system files' records and the root directory's listing of them `(record, name, is_dir)`.
type SystemRecords = (Vec<Vec<u8>>, Vec<(u64, &'static str, bool)>);

/// [`create_ntfs`] as `version`: 3.1 for Windows XP and later, 1.2 for Windows NT 3.5 to 4.0.
pub fn create_ntfs_version<W: Write + Seek>(
    target: &mut W,
    params: &NtfsFormatParams,
    version: NtfsVersion,
) -> Result<(), FilesystemError> {
    let geo = params.geometry;
    geo.validate()?;
    if version == NtfsVersion::V1_2 && geo.bytes_per_sector != 512 {
        return Err(FilesystemError::InvalidData(
            "NTFS 1.2 volumes use 512-byte sectors".into(),
        ));
    }
    let cluster = geo.cluster_size();
    let total_size = params.total_size;
    if !total_size.is_multiple_of(cluster) {
        return Err(FilesystemError::InvalidData(format!(
            "NTFS format: target size {total_size} is not a multiple of the {cluster}-byte cluster"
        )));
    }
    let spc = geo.sectors_per_cluster as u64;
    let bps = geo.bytes_per_sector as u64;
    let rs = geo.mft_record_size as u64;
    let isize = geo.index_record_size as u64;
    let mft_records = params.mft_records_hint.max(64);
    let tc = total_size / cluster; // total clusters
    let total_sectors = tc * spc - 1; // last sector holds the backup boot

    // 1.2 has no $Secure stream and keeps the root's descriptor resident.
    let (attrdef, upcase, sds, rootsd): (&[u8], &[u8], &[u8], &[u8]) = match version {
        NtfsVersion::V3_1 => (ATTRDEF, UPCASE, SECURE_SDS, ROOT_SECDESC),
        NtfsVersion::V1_2 => (NT4_ATTRDEF, NT4_UPCASE, &[], &[]),
    };

    // ---- Contiguous metadata layout ----
    let boot_c = cdiv(8192, cluster);
    let mftbm_real = cdiv(mft_records, 8);
    let mftbm_c = cdiv(mftbm_real, cluster).max(1);
    let mft_real = mft_records * rs;
    let mft_c = cdiv(mft_real, cluster);
    let mftmirr_real = 4 * rs;
    let mftmirr_c = cdiv(mftmirr_real, cluster);
    let log_size = logfile_size(total_size, cluster);
    let log_c = log_size / cluster;
    let clusterbm_real = cdiv(tc, 8);
    let bm_c = cdiv(clusterbm_real, cluster).max(1);
    let attrdef_c = cdiv(attrdef.len() as u64, cluster);
    let upcase_c = cdiv(upcase.len() as u64, cluster);
    let sds_c = cdiv(sds.len() as u64, cluster);
    let rootsd_c = cdiv(rootsd.len() as u64, cluster);
    let rootidx_c = cdiv(isize, cluster);

    let mut lcn = boot_c;
    let mut take = |n: u64| {
        let at = lcn;
        lcn += n;
        at
    };
    let l = Layout {
        cluster,
        tc,
        isize,
        boot_c,
        mftbm_c,
        mftbm_lcn: take(mftbm_c),
        mftbm_real,
        mft_c,
        mft_lcn: take(mft_c),
        mft_real,
        mftmirr_c,
        mftmirr_lcn: take(mftmirr_c),
        mftmirr_real,
        log_c,
        log_lcn: take(log_c),
        log_size,
        bm_c,
        bm_lcn: take(bm_c),
        clusterbm_real,
        attrdef_c,
        attrdef_lcn: take(attrdef_c),
        upcase_c,
        upcase_lcn: take(upcase_c),
        sds_c,
        sds_lcn: take(sds_c),
        rootsd_c,
        rootsd_lcn: take(rootsd_c),
        rootidx_c,
        rootidx_lcn: take(rootidx_c),
    };
    let end_meta = lcn;
    if end_meta >= tc {
        return Err(FilesystemError::DiskFull(format!(
            "NTFS format: metadata needs {end_meta} clusters but the volume only has {tc}"
        )));
    }

    let label = params.label.as_deref();
    let (mut recs, listing) = match version {
        NtfsVersion::V3_1 => system_records_v31(&geo, &l, label),
        NtfsVersion::V1_2 => system_records_v12(&geo, &l, label),
    };
    // The rest: reserved, then free (sequence 0).
    for n in recs.len() as u64..mft_records {
        recs.push(blank_record(version, n, &geo));
    }
    let sys_flags = FA_HIDDEN | FA_SYSTEM;
    let root_entries: Vec<(u64, Vec<u8>)> = listing
        .iter()
        .map(|&(n, name, is_dir)| {
            let attrs = sys_flags | if is_dir { FA_DIR_INDEX } else { 0 };
            (mft_ref(n), file_name_value(name, attrs))
        })
        .collect();
    let root_indx = build_root_index_block(&geo, &root_entries);

    // ---- Cluster allocation bitmap (bit set = in use) ----
    let mut bm = vec![0u8; clusterbm_real as usize];
    for c in 0..end_meta {
        bm[(c / 8) as usize] |= 1 << (c % 8);
    }
    // The last cluster holds the backup boot sector; keep the allocator off it.
    bm[((tc - 1) / 8) as usize] |= 1 << ((tc - 1) % 8);
    for c in tc..(bm.len() as u64 * 8) {
        bm[(c / 8) as usize] |= 1 << (c % 8);
    }

    // ---- MFT record bitmap (the reserved system records) ----
    let mut mftbm = vec![0u8; mftbm_real as usize];
    for i in 0..version.reserved_records() {
        mftbm[(i / 8) as usize] |= 1 << (i % 8);
    }
    for c in mft_records..(mftbm.len() as u64 * 8) {
        mftbm[(c / 8) as usize] |= 1 << (c % 8);
    }

    // ---- Boot sector ----
    let serial = total_size.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (params.label.as_deref().unwrap_or("").len() as u64).wrapping_mul(0xD1B5_4A32_D192_ED03)
        ^ 0x4E54_4653_4E54_4653;
    let boot = build_boot(&geo, total_sectors, l.mft_lcn, l.mftmirr_lcn, serial);

    // ---- Emit ----
    let put = |target: &mut W, lcn0: u64, data: &[u8]| -> Result<(), FilesystemError> {
        target.seek(SeekFrom::Start(lcn0 * cluster))?;
        target.write_all(data)?;
        Ok(())
    };
    // Pre-size so trailing free space reads back as zero.
    target.seek(SeekFrom::Start(total_size - 1))?;
    target.write_all(&[0u8])?;

    put(target, 0, &boot)?;
    put(target, l.mftbm_lcn, &mftbm)?;
    let mut mft_data = Vec::with_capacity(recs.len() * rs as usize);
    for r in &recs {
        mft_data.extend_from_slice(r);
    }
    put(target, l.mft_lcn, &mft_data)?;
    let mut mirror = Vec::with_capacity(4 * rs as usize);
    for r in recs.iter().take(4) {
        mirror.extend_from_slice(r);
    }
    put(target, l.mftmirr_lcn, &mirror)?;
    // $LogFile: 0xFF-filled, which Windows and ntfs-3g take as a log to initialise.
    let logfile = vec![0xFFu8; log_size as usize];
    put(target, l.log_lcn, &logfile)?;
    put(target, l.bm_lcn, &bm)?;
    put(target, l.attrdef_lcn, attrdef)?;
    put(target, l.upcase_lcn, upcase)?;
    put(target, l.sds_lcn, sds)?;
    put(target, l.rootsd_lcn, rootsd)?;
    put(target, l.rootidx_lcn, &root_indx)?;
    // Backup boot sector at the very last sector.
    target.seek(SeekFrom::Start(total_sectors * bps))?;
    target.write_all(&boot)?;
    target.flush()?;
    Ok(())
}

/// The NTFS 1.2 system records 0-15 as NT 4.0 lays them out (docs/ntfs_format_cleanroom_notes.md):
/// every metafile carries its own descriptor, 9 is `$Quota`, 11-15 are reserved and unnamed.
fn system_records_v12(geo: &NtfsGeometry, l: &Layout, label: Option<&str>) -> SystemRecords {
    let v = NtfsVersion::V1_2;
    let sys = FA_HIDDEN | FA_SYSTEM;
    let si = || resident_attr(0x10, "", &std_info_v12(sys), 0, false);
    let fname = |name: &str, alloc: u64, real: u64| {
        resident_attr(
            0x30,
            "",
            &file_name_value_sized(name, sys, alloc, real),
            1,
            true,
        )
    };
    let sd = |desc: &[u8]| resident_attr(0x50, "", desc, 2, false);
    let data = |c: u64, at: u64, real: u64, instance: u16| {
        nonresident_attr(
            0x80,
            "",
            &runlist(c, Some(at)),
            c - 1,
            c * l.cluster,
            real,
            instance,
            0,
        )
    };
    let file = |n: u64, attrs: &[Vec<u8>]| build_record_as(v, n, 0x01, 1, geo, attrs);
    let mut recs = Vec::with_capacity(16);

    // 0: $MFT
    let mft_alloc = l.mft_c * l.cluster;
    recs.push(file(
        0,
        &[
            si(),
            fname("$MFT", mft_alloc, l.mft_real),
            sd(NT4_SD_METAFILE),
            data(l.mft_c, l.mft_lcn, l.mft_real, 3),
            nonresident_attr(
                0xB0,
                "",
                &runlist(l.mftbm_c, Some(l.mftbm_lcn)),
                l.mftbm_c - 1,
                l.mftbm_c * l.cluster,
                l.mftbm_real,
                4,
                0,
            ),
        ],
    ));
    // 1: $MFTMirr
    let mirr_alloc = l.mftmirr_c * l.cluster;
    recs.push(file(
        1,
        &[
            si(),
            fname("$MFTMirr", mirr_alloc, l.mftmirr_real),
            sd(NT4_SD_METAFILE),
            data(l.mftmirr_c, l.mftmirr_lcn, l.mftmirr_real, 3),
        ],
    ));
    // 2: $LogFile
    recs.push(file(
        2,
        &[
            si(),
            fname("$LogFile", l.log_size, l.log_size),
            sd(NT4_SD_METAFILE),
            data(l.log_c, l.log_lcn, l.log_size, 3),
        ],
    ));
    // 3: $Volume
    recs.push(file(
        3,
        &[
            si(),
            fname("$Volume", 0, 0),
            sd(NT4_SD_METAFILE_RW),
            resident_attr(0x60, "", &volume_name_value(label.unwrap_or("")), 3, false),
            resident_attr(0x70, "", &volume_info_value(v), 4, false),
            resident_attr(0x80, "", &[], 5, false),
        ],
    ));
    // 4: $AttrDef
    let attrdef_real = NT4_ATTRDEF.len() as u64;
    recs.push(file(
        4,
        &[
            si(),
            fname("$AttrDef", l.attrdef_c * l.cluster, attrdef_real),
            sd(NT4_SD_METAFILE),
            data(l.attrdef_c, l.attrdef_lcn, attrdef_real, 3),
        ],
    ));
    // 5: . (root directory); its index entries live in the $INDEX_ALLOCATION block.
    recs.push(build_record_as(
        v,
        5,
        0x03, // in use | directory
        1,
        geo,
        &[
            si(),
            resident_attr(0x30, "", &file_name_value(".", sys | FA_DIR_INDEX), 1, true),
            sd(NT4_SD_ROOT),
            resident_attr(0x90, "$I30", &root_index_root(geo), 3, false),
            nonresident_attr(
                0xA0,
                "$I30",
                &runlist(l.rootidx_c, Some(l.rootidx_lcn)),
                l.rootidx_c - 1,
                l.rootidx_c * l.cluster,
                l.isize,
                4,
                0,
            ),
            resident_attr(0xB0, "$I30", &[1, 0, 0, 0, 0, 0, 0, 0], 5, false),
        ],
    ));
    // 6: $Bitmap
    let bm_alloc = l.bm_c * l.cluster;
    recs.push(file(
        6,
        &[
            si(),
            fname("$Bitmap", bm_alloc, l.clusterbm_real),
            sd(NT4_SD_METAFILE),
            data(l.bm_c, l.bm_lcn, l.clusterbm_real, 3),
        ],
    ));
    // 7: $Boot
    recs.push(file(
        7,
        &[
            si(),
            fname("$Boot", l.boot_c * l.cluster, 8192),
            sd(NT4_SD_METAFILE),
            data(l.boot_c, 0, 8192, 3),
        ],
    ));
    // 8: $BadClus (sparse $Bad spanning the whole volume)
    recs.push(file(
        8,
        &[
            si(),
            fname("$BadClus", 0, 0),
            sd(NT4_SD_METAFILE),
            resident_attr(0x80, "", &[], 3, false),
            nonresident_attr(
                0x80,
                "$Bad",
                &runlist(l.tc, None),
                l.tc - 1,
                l.tc * l.cluster,
                l.tc * l.cluster,
                4,
                0,
            ),
        ],
    ));
    // 9: $Quota (empty on NT 4.0)
    recs.push(file(
        9,
        &[
            si(),
            fname("$Quota", 0, 0),
            sd(NT4_SD_METAFILE_RW),
            resident_attr(0x80, "", &[], 3, false),
        ],
    ));
    // 10: $UpCase (no $Info stream before NTFS 3)
    let upcase_real = NT4_UPCASE.len() as u64;
    recs.push(file(
        10,
        &[
            si(),
            fname("$UpCase", l.upcase_c * l.cluster, upcase_real),
            sd(NT4_SD_METAFILE),
            data(l.upcase_c, l.upcase_lcn, upcase_real, 3),
        ],
    ));
    // 11-15: reserved; in use with no name and no links, as NT 4.0 formats them.
    for n in 11..16 {
        recs.push(build_record_as(
            v,
            n,
            0x01,
            0,
            geo,
            &[
                si(),
                resident_attr(0x50, "", NT4_SD_METAFILE_RW, 1, false),
                resident_attr(0x80, "", &[], 2, false),
            ],
        ));
    }
    let listing = vec![
        (4, "$AttrDef", false),
        (8, "$BadClus", false),
        (6, "$Bitmap", false),
        (7, "$Boot", false),
        (2, "$LogFile", false),
        (0, "$MFT", false),
        (1, "$MFTMirr", false),
        (9, "$Quota", false),
        (10, "$UpCase", false),
        (3, "$Volume", false),
        (5, ".", true),
    ];
    (recs, listing)
}

/// The NTFS 3.1 system records 0-11 ($Secure, $Extend) and the root listing.
fn system_records_v31(geo: &NtfsGeometry, l: &Layout, label: Option<&str>) -> SystemRecords {
    let geo = *geo;
    let Layout {
        cluster,
        tc,
        isize,
        boot_c,
        mftbm_c,
        mftbm_lcn,
        mftbm_real,
        mft_c,
        mft_lcn,
        mft_real,
        mftmirr_c,
        mftmirr_lcn,
        mftmirr_real,
        log_c,
        log_lcn,
        log_size,
        bm_c,
        bm_lcn,
        clusterbm_real,
        attrdef_c,
        attrdef_lcn,
        upcase_c,
        upcase_lcn,
        sds_c,
        sds_lcn,
        rootsd_c,
        rootsd_lcn,
        rootidx_c,
        rootidx_lcn,
    } = *l;
    // ---- System MFT records 0..11 ----
    let sys_flags = FA_HIDDEN | FA_SYSTEM; // 0x06
    let mut recs: Vec<Vec<u8>> = Vec::with_capacity(16);

    // 0: $MFT
    recs.push(build_record(
        0,
        0x01,
        &geo,
        &[
            resident_attr(0x10, "", &std_info(sys_flags, SECURITY_ID), 0, false),
            resident_attr(0x30, "", &file_name_value("$MFT", sys_flags), 1, true),
            nonresident_attr(
                0x80,
                "",
                &runlist(mft_c, Some(mft_lcn)),
                mft_c - 1,
                mft_c * cluster,
                mft_real,
                2,
                0,
            ),
            nonresident_attr(
                0xB0,
                "",
                &runlist(mftbm_c, Some(mftbm_lcn)),
                mftbm_c - 1,
                mftbm_c * cluster,
                mftbm_real,
                3,
                0,
            ),
        ],
    ));
    // 1: $MFTMirr
    recs.push(build_record(
        1,
        0x01,
        &geo,
        &[
            resident_attr(0x10, "", &std_info(sys_flags, SECURITY_ID), 0, false),
            resident_attr(0x30, "", &file_name_value("$MFTMirr", sys_flags), 1, true),
            nonresident_attr(
                0x80,
                "",
                &runlist(mftmirr_c, Some(mftmirr_lcn)),
                mftmirr_c - 1,
                mftmirr_c * cluster,
                mftmirr_real,
                2,
                0,
            ),
        ],
    ));
    // 2: $LogFile
    recs.push(build_record(
        2,
        0x01,
        &geo,
        &[
            resident_attr(0x10, "", &std_info(sys_flags, SECURITY_ID), 0, false),
            resident_attr(0x30, "", &file_name_value("$LogFile", sys_flags), 1, true),
            nonresident_attr(
                0x80,
                "",
                &runlist(log_c, Some(log_lcn)),
                log_c - 1,
                log_size,
                log_size,
                2,
                0,
            ),
        ],
    ));
    // 3: $Volume
    {
        let label = label.unwrap_or("");
        recs.push(build_record(
            3,
            0x01,
            &geo,
            &[
                resident_attr(0x10, "", &std_info(sys_flags, 0), 0, false),
                resident_attr(0x30, "", &file_name_value("$Volume", sys_flags), 1, true),
                resident_attr(0x50, "", SEC_DESC_100, 2, false),
                resident_attr(0x60, "", &volume_name_value(label), 3, false),
                resident_attr(0x70, "", &volume_info_value(NtfsVersion::V3_1), 4, false),
                resident_attr(0x80, "", &[], 5, false),
            ],
        ));
    }
    // 4: $AttrDef
    recs.push(build_record(
        4,
        0x01,
        &geo,
        &[
            resident_attr(0x10, "", &std_info(sys_flags, 0), 0, false),
            resident_attr(0x30, "", &file_name_value("$AttrDef", sys_flags), 1, true),
            resident_attr(0x50, "", SEC_DESC_100, 2, false),
            nonresident_attr(
                0x80,
                "",
                &runlist(attrdef_c, Some(attrdef_lcn)),
                attrdef_c - 1,
                attrdef_c * cluster,
                ATTRDEF.len() as u64,
                3,
                0,
            ),
        ],
    ));
    // 5: . (root directory). The 13 root index entries are emitted to the
    // $INDEX_ALLOCATION INDX block below; the record itself only carries the
    // resident "large" $INDEX_ROOT pointing at that block.
    {
        recs.push(build_record(
            5,
            0x03, // in use | directory
            &geo,
            &[
                // The root is what every created file inherits from, so it needs a real id.
                resident_attr(0x10, "", &std_info(sys_flags, SECURITY_ID), 0, false),
                resident_attr(
                    0x30,
                    "",
                    &file_name_value(".", sys_flags | FA_DIR_INDEX),
                    1,
                    true,
                ),
                nonresident_attr(
                    0x50,
                    "",
                    &runlist(rootsd_c, Some(rootsd_lcn)),
                    rootsd_c - 1,
                    rootsd_c * cluster,
                    ROOT_SECDESC.len() as u64,
                    2,
                    0,
                ),
                resident_attr(0x90, "$I30", &root_index_root(&geo), 3, false),
                nonresident_attr(
                    0xA0,
                    "$I30",
                    &runlist(rootidx_c, Some(rootidx_lcn)),
                    rootidx_c - 1,
                    rootidx_c * cluster,
                    isize,
                    4,
                    0,
                ),
                resident_attr(0xB0, "$I30", &[1, 0, 0, 0, 0, 0, 0, 0], 5, false),
            ],
        ));
    }
    // 6: $Bitmap
    recs.push(build_record(
        6,
        0x01,
        &geo,
        &[
            resident_attr(0x10, "", &std_info(sys_flags, SECURITY_ID), 0, false),
            resident_attr(0x30, "", &file_name_value("$Bitmap", sys_flags), 1, true),
            nonresident_attr(
                0x80,
                "",
                &runlist(bm_c, Some(bm_lcn)),
                bm_c - 1,
                bm_c * cluster,
                clusterbm_real,
                2,
                0,
            ),
        ],
    ));
    // 7: $Boot
    recs.push(build_record(
        7,
        0x01,
        &geo,
        &[
            resident_attr(0x10, "", &std_info(sys_flags, 0), 0, false),
            resident_attr(0x30, "", &file_name_value("$Boot", sys_flags), 1, true),
            resident_attr(0x50, "", SEC_DESC_100, 2, false),
            nonresident_attr(
                0x80,
                "",
                &runlist(boot_c, Some(0)),
                boot_c - 1,
                boot_c * cluster,
                8192,
                3,
                0,
            ),
        ],
    ));
    // 8: $BadClus (sparse $Bad spanning the whole volume)
    recs.push(build_record(
        8,
        0x01,
        &geo,
        &[
            resident_attr(0x10, "", &std_info(sys_flags, SECURITY_ID), 0, false),
            resident_attr(0x30, "", &file_name_value("$BadClus", sys_flags), 1, true),
            resident_attr(0x80, "", &[], 2, false),
            nonresident_attr(
                0x80,
                "$Bad",
                &runlist(tc, None),
                tc - 1,
                tc * cluster,
                tc * cluster,
                3,
                0,
            ),
        ],
    ));
    // 9: $Secure
    recs.push(build_record(
        9,
        0x09, // in use | view index
        &geo,
        &[
            resident_attr(0x10, "", &std_info(sys_flags, SECURITY_ID), 0, false),
            resident_attr(0x30, "", &file_name_value("$Secure", sys_flags), 1, true),
            nonresident_attr(
                0x80,
                "$SDS",
                &runlist(sds_c, Some(sds_lcn)),
                sds_c - 1,
                sds_c * cluster,
                SECURE_SDS.len() as u64,
                2,
                0,
            ),
            resident_attr(0x90, "$SDH", SECURE_SDH_IDXROOT, 3, false),
            resident_attr(0x90, "$SII", SECURE_SII_IDXROOT, 4, false),
        ],
    ));
    // 10: $UpCase
    recs.push(build_record(
        10,
        0x01,
        &geo,
        &[
            resident_attr(0x10, "", &std_info(sys_flags, SECURITY_ID), 0, false),
            resident_attr(0x30, "", &file_name_value("$UpCase", sys_flags), 1, true),
            nonresident_attr(
                0x80,
                "",
                &runlist(upcase_c, Some(upcase_lcn)),
                upcase_c - 1,
                upcase_c * cluster,
                UPCASE.len() as u64,
                2,
                0,
            ),
            resident_attr(0x80, "$Info", UPCASE_INFO, 3, false),
        ],
    ));
    // 11: $Extend (empty directory)
    recs.push(build_record(
        11,
        0x03, // in use | directory
        &geo,
        &[
            resident_attr(0x10, "", &std_info(sys_flags, SECURITY_ID), 0, false),
            resident_attr(
                0x30,
                "",
                &file_name_value("$Extend", sys_flags | FA_DIR_INDEX),
                1,
                true,
            ),
            resident_attr(0x90, "$I30", &empty_index_root(&geo), 2, false),
        ],
    ));
    let listing = vec![
        (4, "$AttrDef", false),
        (8, "$BadClus", false),
        (6, "$Bitmap", false),
        (7, "$Boot", false),
        (11, "$Extend", true),
        (2, "$LogFile", false),
        (0, "$MFT", false),
        (1, "$MFTMirr", false),
        (9, "$Secure", false),
        (10, "$UpCase", false),
        (3, "$Volume", false),
        (5, ".", true),
    ];
    (recs, listing)
}

/// Back-compat shim: format with the legacy fixed geometry (512-byte sectors,
/// 4096-byte clusters, 1024-byte MFT records). Prefer [`create_ntfs`].
pub fn create_blank_ntfs<W: Write + Seek>(
    target: &mut W,
    target_size: u64,
    mft_records: u64,
    label: Option<&str>,
) -> Result<(), FilesystemError> {
    let geometry = NtfsGeometry::with_cluster_size(4096, 512)?;
    create_ntfs(
        target,
        &NtfsFormatParams {
            total_size: target_size,
            geometry,
            mft_records_hint: mft_records,
            label: label.map(|s| s.to_string()),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ntfs::NtfsFilesystem;
    use disk_core::filesystem::{
        CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
    };
    use std::io::Cursor;

    #[test]
    fn geometry_defaults_and_validation() {
        assert_eq!(
            NtfsGeometry::for_volume_size(100 * 1024 * 1024).cluster_size(),
            512
        );
        assert_eq!(
            NtfsGeometry::for_volume_size(3 * 1024 * 1024 * 1024).cluster_size(),
            4096
        );
        assert!(NtfsGeometry::with_cluster_size(4096, 512).is_ok());
        assert!(NtfsGeometry::with_cluster_size(65536, 512).is_ok());
        assert!(NtfsGeometry::with_cluster_size(300, 512).is_err()); // not power of two
        assert!(NtfsGeometry::with_cluster_size(256, 512).is_err()); // < sector
        assert!(NtfsGeometry::with_cluster_size(4096, 3000).is_err()); // bad sector
        assert!(NtfsGeometry::with_cluster_size(4 * 1024 * 1024, 512).is_err());
        // > 2 MiB
    }

    fn format(size: u64, geo: NtfsGeometry, label: &str) -> Cursor<Vec<u8>> {
        let mut cur = Cursor::new(Vec::<u8>::new());
        create_ntfs(
            &mut cur,
            &NtfsFormatParams {
                total_size: size,
                geometry: geo,
                mft_records_hint: 128,
                label: Some(label.to_string()),
            },
        )
        .expect("format");
        cur.seek(SeekFrom::Start(0)).unwrap();
        cur
    }

    #[test]
    fn blank_ntfs_opens_and_is_writable_by_our_reader() {
        let size = 32 * 1024 * 1024u64;
        let cur = format(
            size,
            NtfsGeometry::with_cluster_size(4096, 512).unwrap(),
            "RBNTFS",
        );
        assert_eq!(cur.get_ref().len() as u64, size);

        let mut fs = NtfsFilesystem::open(cur, 0).expect("our reader opens the blank");
        assert_eq!(fs.volume_label(), Some("RBNTFS"));
        let root = fs.root().unwrap();
        assert!(
            fs.list_directory(&root).unwrap().is_empty(),
            "fresh volume has an empty root"
        );

        let data = vec![0x33u8; 10_000];
        let mut src = Cursor::new(data.clone());
        fs.create_file(
            &root,
            "data.bin",
            &mut src,
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file into the blank");
        EditableFilesystem::sync_metadata(&mut fs).unwrap();

        let entries = fs.list_directory(&root).unwrap();
        let f = entries
            .iter()
            .find(|e| e.name == "data.bin")
            .expect("file present after sync");
        assert_eq!(fs.read_file(f, f.size as usize).unwrap(), data);
    }

    fn format_as(size: u64, cluster: u32, version: NtfsVersion) -> Cursor<Vec<u8>> {
        let mut cur = Cursor::new(Vec::<u8>::new());
        create_ntfs_version(
            &mut cur,
            &NtfsFormatParams {
                total_size: size,
                geometry: NtfsGeometry::with_cluster_size(cluster, 512).unwrap(),
                mft_records_hint: 128,
                label: Some("NT4".to_string()),
            },
            version,
        )
        .expect("format");
        cur.seek(SeekFrom::Start(0)).unwrap();
        cur
    }

    /// Each in-use MFT record, read straight off the image: (number, fixup-array offset,
    /// `$STANDARD_INFORMATION` length if it has one). Independent of our reader.
    fn raw_records(img: &[u8]) -> Vec<(u64, u16, Option<u32>)> {
        let u16_at = |b: &[u8], o: usize| u16::from_le_bytes([b[o], b[o + 1]]);
        let u32_at = |b: &[u8], o: usize| u32::from_le_bytes(b[o..o + 4].try_into().unwrap());
        let cluster = u16_at(img, 0x0B) as u64 * img[0x0D] as u64;
        let mft = u64::from_le_bytes(img[0x30..0x38].try_into().unwrap()) * cluster;
        let mut out = Vec::new();
        for n in 0..128u64 {
            let at = (mft + n * 1024) as usize;
            let mut r = img[at..at + 1024].to_vec();
            if &r[0..4] != b"FILE" || u16_at(&r, 0x16) & 1 == 0 {
                continue;
            }
            let (usa, cnt) = (u16_at(&r, 4) as usize, u16_at(&r, 6) as usize);
            for i in 1..cnt {
                let (end, saved) = (i * 512 - 2, usa + 2 * i);
                r.copy_within(saved..saved + 2, end);
            }
            let mut o = u16_at(&r, 0x14) as usize;
            let mut si = None;
            while u32_at(&r, o) != 0xFFFF_FFFF {
                if u32_at(&r, o) == 0x10 {
                    si = Some(u32_at(&r, o + 0x10));
                }
                o += u32_at(&r, o + 4) as usize;
            }
            out.push((n, usa as u16, si));
        }
        out
    }

    #[test]
    fn ntfs_12_matches_nt4_and_round_trips_at_each_nt4_cluster() {
        for cluster in [512u32, 1024, 2048, 4096] {
            let cur = format_as(16 * 1024 * 1024, cluster, NtfsVersion::V1_2);
            let mut fs = NtfsFilesystem::open(cur, 0)
                .unwrap_or_else(|e| panic!("open 1.2 cluster={cluster}: {e:?}"));
            assert_eq!(fs.ntfs_version(), (1, 2), "cluster={cluster}");
            let root = fs.root().unwrap();
            assert!(fs.list_directory(&root).unwrap().is_empty());
            let dir = fs
                .create_directory(&root, "SUB", &CreateDirectoryOptions::default())
                .unwrap();
            let body = vec![0xA5u8; 70_000];
            let opts = CreateFileOptions::default();
            fs.create_file(
                &dir,
                "BIG.BIN",
                &mut Cursor::new(body.clone()),
                70_000,
                &opts,
            )
            .unwrap_or_else(|e| panic!("create_file cluster={cluster}: {e:?}"));
            fs.create_file(
                &root,
                "SMALL.TXT",
                &mut Cursor::new(b"hi".to_vec()),
                2,
                &opts,
            )
            .unwrap();
            EditableFilesystem::sync_metadata(&mut fs).unwrap();

            let img = fs.into_reader().into_inner();
            let mut fs = NtfsFilesystem::open(Cursor::new(img.clone()), 0).unwrap();
            assert_eq!(fs.ntfs_version(), (1, 2), "still 1.2 after writes");
            let root = fs.root().unwrap();
            let names: Vec<String> = fs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .map(|e| e.name)
                .collect();
            assert!(names.contains(&"SUB".to_string()) && names.contains(&"SMALL.TXT".to_string()));
            let sub = fs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "SUB");
            let big = fs.list_directory(&sub.unwrap()).unwrap().remove(0);
            assert_eq!(fs.read_file(&big, big.size as usize).unwrap(), body);

            let recs = raw_records(&img);
            assert!(
                recs.len() >= 19,
                "16 system records plus three new ones: {recs:?}"
            );
            for (n, usa, si) in recs {
                assert_eq!(
                    usa, 0x2A,
                    "record {n}: the 1.2 header puts the fixups at 0x2A"
                );
                if !(11..16).contains(&n) {
                    assert_eq!(si, Some(48), "record {n}: a 48-byte $STANDARD_INFORMATION");
                }
            }
        }
    }

    #[test]
    fn ntfs_12_system_files_are_nt4s() {
        let cur = format_as(16 * 1024 * 1024, 512, NtfsVersion::V1_2);
        let img = cur.into_inner();
        let recs = raw_records(&img);
        let numbers: Vec<u64> = recs.iter().map(|r| r.0).collect();
        assert_eq!(
            numbers,
            (0..16).collect::<Vec<_>>(),
            "records 0-15 in use, as on NT 4.0"
        );
        let mut fs = NtfsFilesystem::open(Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        // The system files hide from listings; no $Secure and no $Extend before NTFS 3.
        let all = fs.list_directory(&root).unwrap();
        assert!(all
            .iter()
            .all(|e| e.name != "$Secure" && e.name != "$Extend"));
    }

    #[test]
    fn ntfs_12_refuses_other_sector_sizes_and_flags_big_clusters() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        let params = NtfsFormatParams {
            total_size: 16 * 1024 * 1024,
            geometry: NtfsGeometry::with_cluster_size(4096, 4096).unwrap(),
            mft_records_hint: 128,
            label: None,
        };
        assert!(create_ntfs_version(&mut cur, &params, NtfsVersion::V1_2).is_err());
        let big = NtfsGeometry::with_cluster_size(8192, 512).unwrap();
        assert!(NtfsVersion::V1_2.geometry_problem(&big).is_some());
        assert!(NtfsVersion::V3_1.geometry_problem(&big).is_none());
        let fine = NtfsGeometry::with_cluster_size(4096, 512).unwrap();
        assert!(NtfsVersion::V1_2.geometry_problem(&fine).is_none());
    }

    #[test]
    fn ntfs_12_formats_a_sparse_4_gib_volume() {
        let size = 4 * 1024 * 1024 * 1024u64;
        let mut f = tempfile::tempfile().unwrap();
        f.set_len(size).unwrap();
        let geometry = NtfsGeometry::for_volume_size(size);
        assert_eq!(
            geometry.cluster_size(),
            4096,
            "NT 4.0's default above 2 GiB"
        );
        let params = NtfsFormatParams {
            total_size: size,
            geometry,
            mft_records_hint: 1024,
            label: Some("BIG".into()),
        };
        create_ntfs_version(&mut f, &params, NtfsVersion::V1_2).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        let mut fs = NtfsFilesystem::open(f, 0).unwrap();
        assert_eq!(fs.ntfs_version(), (1, 2));
        let root = fs.root().unwrap();
        let opts = CreateFileOptions::default();
        fs.create_file(&root, "A.TXT", &mut Cursor::new(b"abc".to_vec()), 3, &opts)
            .unwrap();
        EditableFilesystem::sync_metadata(&mut fs).unwrap();
        let e = fs.list_directory(&root).unwrap().remove(0);
        assert_eq!(fs.read_file(&e, 3).unwrap(), b"abc");
    }

    #[test]
    fn default_31_is_still_31() {
        let fs = NtfsFilesystem::open(format_as(8 * 1024 * 1024, 4096, NtfsVersion::V3_1), 0);
        assert_eq!(fs.unwrap().ntfs_version(), (3, 1));
    }

    #[test]
    fn our_reader_round_trips_across_geometry() {
        for (cluster, sector) in [
            (512u32, 512u32),
            (1024, 512),
            (4096, 512),
            (8192, 512),
            (65536, 512),
            (4096, 4096),
            (8192, 4096),
            (65536, 4096),
        ] {
            let geo = NtfsGeometry::with_cluster_size(cluster, sector).unwrap();
            let size = 64 * 1024 * 1024u64;
            let cur = format(size, geo, "RBGEO");
            let mut fs = NtfsFilesystem::open(cur, 0)
                .unwrap_or_else(|e| panic!("reader open cluster={cluster}: {e:?}"));
            let root = fs.root().unwrap();
            assert!(
                fs.list_directory(&root).unwrap().is_empty(),
                "cluster={cluster}"
            );
            let big = vec![0x5Au8; 40_000];
            let mut src = Cursor::new(big.clone());
            fs.create_file(
                &root,
                "big.bin",
                &mut src,
                big.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap_or_else(|e| panic!("create_file cluster={cluster}: {e:?}"));
            EditableFilesystem::sync_metadata(&mut fs).unwrap();
            let entries = fs.list_directory(&root).unwrap();
            let f = entries
                .iter()
                .find(|e| e.name == "big.bin")
                .expect("present");
            assert_eq!(
                fs.read_file(f, f.size as usize).unwrap(),
                big,
                "cluster={cluster}"
            );
        }
    }
}
