//! Pilot / Cedar nucleus filesystem (Xerox D-machine: Dolphin / Dorado /
//! Dandelion).
//!
//! Structurally unrelated to the Alto BFS that shares this directory, but it
//! reuses the same label-aware [`Disk`] medium and the PDI container
//! (`fsFamily = 2`, 512-byte data pages, 20-byte labels). The on-disk format is
//! defined normatively in `~/docs/PARC_PILOT_FORMAT.md`, grounded in the
//! Cedar-nucleus + original-Pilot Mesa sources and the disk microcode.
//!
//! Per-sector label (10 words): `fileID`(0-4) / `filePage`(5 lo, 6 hi) /
//! `attributes`(7) / `dontCare`(8-9). 32-bit (`INT`/`LONG`) fields are stored
//! **low-word-first** (microcode-confirmed); each 16-bit word is big-endian in
//! our [`Disk`] model. Two file-ID generations share the 5-word `fileID` slot,
//! selected by PDI `flags` bit 2: the Cedar nucleus `File.FP` (32-bit `FileID`
//! + 32-bit `DA` hint + zero fill) and the original-Pilot 80-bit `UniversalID`.
//!
//! Implemented: the structure codec; a from-scratch blank-volume creator
//! (Othello-exact placement, both generations, real VAM file); add / delete
//! files (multi-run allocation); a [`Filesystem`]-trait read/browse view; all
//! validated by round-trip (there is no period Pilot disk to validate against,
//! by design — see the handoff).
//!
//! Sources: `VolumeFormat.mesa`, `DiskFace.mesa`, `File.mesa`, `Checksum.mesa`,
//! `PilotDiskDefs.mc`, and the Othello `PhysicalVolumeScavenger` (placement).

use std::collections::HashMap;

use super::super::entry::FileEntry;
use super::super::filesystem::{Filesystem, FilesystemError};
use super::{be16, put_be16, Disk, FsFamily, Geometry, Sector};

/// Data words per page (256 = 512 bytes).
pub const PAGE_WORDS: usize = 256;
const PAGE_BYTES: usize = 512;
const LABEL_BYTES: usize = 20; // 10 words

// Root-page seals (octal in the sources) and current versions.
const PR_SEAL: u16 = 0o121212; // physical volume root
const PR_VERSION: u16 = 6;
const LR_SEAL: u16 = 0o131313; // logical volume root
const LR_VERSION: u16 = 5;
const PSM_SEAL: u16 = 0o141414; // physical subvolume end marker
const PSM_VERSION: u16 = 0;

/// Page-role label `attributes` values (Cedar nucleus).
pub mod attr {
    pub const PHYSICAL_ROOT: u16 = 1;
    pub const BAD_PAGE_LIST: u16 = 2;
    #[allow(dead_code)]
    pub const BAD_PAGE: u16 = 3;
    pub const SUB_VOLUME_MARKER: u16 = 4;
    pub const LOGICAL_ROOT: u16 = 5;
    pub const FREE_PAGE: u16 = 9728;
    pub const HEADER: u16 = 9729;
    pub const DATA: u16 = 9730;

    /// Classic Pilot (`PilotFileTypes`) page kinds, used by pre-Cedar volumes
    /// (e.g. the 6085 Pilot 12.3 disks): label word 7 holds a `File.Type`, where
    /// a free page is `tFreePage = 6` (vs the Cedar-nucleus `FREE_PAGE = 9728`).
    /// Other classic types: VAM = 7, VFM = 8, normal data pages carry the file's
    /// own type. See `PilotFileTypes.mesa`.
    pub const FREE_PAGE_CLASSIC: u16 = 6;
}

/// PilotDisk.mc clears the low three file-flag bits in its private label after
/// the first sector of a file run. Thus a Cedar continuation sector has the
/// `freePage` Attributes value but retains a non-volume RelID. A genuinely
/// free page has both the logical volume's AbsID and that Attributes value.
fn is_free_label(label: &Label, volume_id: [u16; 5], generation: Generation) -> bool {
    match generation {
        Generation::CedarNucleus => {
            label.attributes == attr::FREE_PAGE && label.file_id == volume_id
        }
        Generation::OriginalPilot => {
            label.attributes == attr::FREE_PAGE || label.attributes == attr::FREE_PAGE_CLASSIC
        }
    }
}

const VOLUME_LABEL_LEN: usize = 40;
const MAX_SUBVOLS: usize = 6;
/// `LogicalVolume.rootPageNumber` — the LV root is logical page 0.
const ROOT_PAGE_NUMBER: u32 = 0;
/// Physical-volume overhead reserved before the first subvolume, matching
/// Othello's `FormatTrident.pagesReservedInPartition1 = 3 * 28` (PV root at
/// page 0, bad-page list at page 1, credentials at page 2, then the "Initial"
/// boot-microcode region). The first subvolume — and logical page 0, the LV
/// root — starts at this page. (Empty for a non-boot blank volume.)
const OTHELLO_PV_RESERVE: usize = 3 * 28; // 84

/// `FSPropertiesImpl.validation` — the seal Cedar requires at the start of a
/// file's property storage (its second header page). Cedar computes it as
/// `GVBasics.MakeKey["August 19, 1983 1:16 pm"]` (the source marks that stamp
/// "DON'T CHANGE THIS!!!"), and every property accessor — `GetProps`,
/// `SetProps`, `GetNameBodyAndVersion` — checks `pPtr.validation = validation`
/// first, treating a mismatch as "invalid property page". A file whose
/// property page is blank is therefore skipped by directory enumeration
/// ("Property page of a local file has a bad property page"), even though its
/// name and contents are perfectly good.
///
/// Rather than reimplement Grapevine's key hash, these are the four words as
/// written by Cedar 6.1 itself, read back from files it created during a live
/// install (identical across every such file).
const PROPERTIES_VALIDATION: [u16; 4] = [53958, 60644, 54392, 64546];

/// Word layout of `FSPropertiesImpl.PropertiesObject`, which occupies the
/// second header page:
///   0..3  validation (GVBasics.Password)
///   4..5  bytes      (INT, low word first)
///   6     keep       (CARDINAL)
///   7..8  created    (BasicTime.GMT)
///   9     version    (FSBackdoor.Version)
///   10    nameBody   TextRep length in chars, characters follow 2/word
const PROP_BYTES_WORD: usize = 4;
const PROP_KEEP_WORD: usize = 6;
const PROP_VERSION_WORD: usize = 9;
const PROP_NAME_WORD: usize = 10;

/// Well-known 32-bit FileID of the Volume Allocation Map file (Cedar nucleus
/// `VolumeFile::VAM` = root-file slot 7). We allocate it first, so user files
/// start at FileID 2.
const VAM_FILE_ID: [u16; 5] = [1, 0, 0, 0, 0];
/// Word offset of `rootFile[VolumeFile::VAM]` in the LV root (`rootFile`
/// base 85 + 7 * SIZE[RootFile](6)).
const ROOTFILE_VAM_WORD: usize = 85 + 7 * 6;
/// Word offset of `rootFile[VolumeFile::client]` in the LV root (slot 8): the
/// FS name-directory file (`File.mesa!1` `VolumeFile.client = 8`). Each
/// `RootFile` is `fp: File.FP`(4) + `page: File.PageNumber`(2) = 6 words; the
/// `fp.id` (2-word 32-bit FileID) sits at the slot's word 0.
const ROOTFILE_CLIENT_WORD: usize = 85 + 8 * 6;

/// Words of VAM data for a volume of `volume_size` pages.  Cedar's
/// `LogicalVolumeImpl.VAMWords` rounds the bitmap to 65,536-page chunks:
/// `rover`(2) + `size`(2) + 4096 words per nonempty chunk.  Tight-packing the
/// final chunk makes the file shorter than Cedar's `ReadVAM` request.
fn vam_data_words(volume_size: u32) -> usize {
    4 + (volume_size as usize).div_ceil(1 << 16) * 4096
}
/// Pages the VAM bitmap occupies (256 words each).
fn vam_data_pages(volume_size: u32) -> usize {
    vam_data_words(volume_size).div_ceil(PAGE_WORDS)
}

/// File-ID generation, selected by PDI `flags` bit 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Generation {
    /// Cedar nucleus: `File.FP` = 32-bit `FileID` + 32-bit `DA` hint + fill.
    CedarNucleus,
    /// Original Pilot: 80-bit `UniversalID`.
    OriginalPilot,
}

impl Generation {
    /// True if this generation sets PDI `flags` bit 2 (80-bit file IDs).
    pub fn pdi_flag_bit2(self) -> bool {
        matches!(self, Generation::OriginalPilot)
    }
    pub fn from_pdi_flag_bit2(bit2: bool) -> Self {
        if bit2 {
            Generation::OriginalPilot
        } else {
            Generation::CedarNucleus
        }
    }
}

/// Per-volume creator prefix (label words 2-4) for synthesized original-Pilot
/// 80-bit `UniversalID`s. Non-zero so an original-Pilot file id is visibly an
/// 80-bit UID, distinct from a Cedar `File.FP` (whose word 4 is fill = 0).
const PILOT_UID_CREATOR: [u16; 3] = [0x5242, 0x0000, 0x0001]; // "RB" + v1

/// Build a label `fileID` from a per-volume counter and a file's run-table
/// leader.  DiskFace.mesa defines Cedar's `RelID` as four words, i.e. the
/// complete `File.FP` (`FileID` plus `DA`), not merely the two-word FileID.
/// Original Pilot instead keeps its five-word UniversalID unchanged.
fn make_file_id(generation: Generation, n: u32, da: u32) -> [u16; 5] {
    let (lo, hi) = ((n & 0xffff) as u16, (n >> 16) as u16);
    match generation {
        Generation::CedarNucleus => [lo, hi, (da & 0xffff) as u16, (da >> 16) as u16, 0],
        Generation::OriginalPilot => [
            lo,
            hi,
            PILOT_UID_CREATOR[0],
            PILOT_UID_CREATOR[1],
            PILOT_UID_CREATOR[2],
        ],
    }
}

/// Write a Pilot disk to a PDI image, recording the file-ID generation in
/// `flags` bit 2 so a reader interprets the label `fileID` at the right width.
pub fn write_pdi(disk: &Disk, generation: Generation) -> Vec<u8> {
    let extra = if generation.pdi_flag_bit2() {
        super::pdi::FLAG_PILOT_80BIT
    } else {
        0
    };
    super::pdi::write_with_flags(disk, extra)
}

// ---- word access on a page/label byte buffer (big-endian words) ----

#[inline]
fn rdw(buf: &[u8], wi: usize) -> u16 {
    be16(buf, wi * 2)
}
#[inline]
fn wrw(buf: &mut [u8], wi: usize, v: u16) {
    put_be16(buf, wi * 2, v)
}
/// Read a 32-bit Mesa `LONG`/`INT` at word index `wi` (low word first).
#[inline]
fn rdlong(buf: &[u8], wi: usize) -> u32 {
    (rdw(buf, wi) as u32) | ((rdw(buf, wi + 1) as u32) << 16)
}
/// Write a 32-bit Mesa `LONG`/`INT` at word index `wi` (low word first).
#[inline]
fn wrlong(buf: &mut [u8], wi: usize, v: u32) {
    wrw(buf, wi, (v & 0xffff) as u16);
    wrw(buf, wi + 1, (v >> 16) as u16);
}

/// The Pilot page checksum (`Checksum.ComputeChecksumSoftware`): a 16-bit
/// ones-complement add with a left-rotate after each word, `0xFFFF -> 0`.
pub fn pilot_checksum(words: &[u16]) -> u16 {
    let mut cs: u16 = 0;
    for &w in words {
        let (t, carry) = cs.overflowing_add(w);
        cs = if carry { t.wrapping_add(1) } else { t };
        cs = if cs & 0x8000 != 0 {
            (cs << 1) | 1
        } else {
            cs << 1
        };
    }
    if cs == 0xffff {
        0
    } else {
        cs
    }
}

/// Compute the checksum over a page's words `[0..255)` and store it at word 255.
///
/// Note: real Pilot/Cedar **never computes** the physical/logical volume-root
/// page checksum — the `checksum` field (word 255) is declared `_ 0` and left
/// zero; volume validity is established by the seal + version alone (confirmed
/// against the Dwarf 6085 disks, whose root-page word 255 is 0). We still stamp
/// a checksum on the pages our own writer creates (it is a harmless reserved
/// word that real Pilot ignores), but the reader does not verify it.
fn set_page_checksum(data: &mut [u8]) {
    let words: Vec<u16> = (0..PAGE_WORDS - 1).map(|i| rdw(data, i)).collect();
    wrw(data, PAGE_WORDS - 1, pilot_checksum(&words));
}

/// Pack an ASCII string into a `PACKED ARRAY OF CHARACTER` starting at word
/// `wi` (2 chars/word, first char in the high byte), for `len` words.
fn pack_label(buf: &mut [u8], wi: usize, s: &str, words: usize) {
    let bytes = s.as_bytes();
    for i in 0..words * 2 {
        let b = bytes.get(i).copied().unwrap_or(0);
        buf[wi * 2 + i] = b;
    }
}
fn unpack_label(buf: &[u8], wi: usize, len: usize) -> String {
    let start = wi * 2;
    let raw = &buf[start..start + len.min(VOLUME_LABEL_LEN)];
    String::from_utf8_lossy(raw)
        .trim_end_matches('\0')
        .to_string()
}

// ---- label ----

/// A parsed 10-word `DiskFace.Label`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Label {
    pub file_id: [u16; 5],
    pub file_page: u32,
    pub attributes: u16,
    pub dont_care: [u16; 2],
}

impl Label {
    pub fn parse(label: &[u8]) -> Self {
        let mut file_id = [0u16; 5];
        for (i, w) in file_id.iter_mut().enumerate() {
            *w = rdw(label, i);
        }
        Label {
            file_id,
            file_page: rdlong(label, 5),
            attributes: rdw(label, 7),
            dont_care: [rdw(label, 8), rdw(label, 9)],
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        let mut l = vec![0u8; LABEL_BYTES];
        for (i, w) in self.file_id.iter().enumerate() {
            wrw(&mut l, i, *w);
        }
        wrlong(&mut l, 5, self.file_page);
        wrw(&mut l, 7, self.attributes);
        wrw(&mut l, 8, self.dont_care[0]);
        wrw(&mut l, 9, self.dont_care[1]);
        l
    }

    fn new(file_id: [u16; 5], file_page: u32, attributes: u16) -> Self {
        Label {
            file_id,
            file_page,
            attributes,
            dont_care: [0, 0],
        }
    }
}

// ---- subvolume descriptor (13 words) ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubVolumeDesc {
    pub lv_id: [u16; 5],
    pub lv_size: u32,
    pub lv_page: u32,
    pub pv_page: u32,
    pub n_pages: u32,
}

impl SubVolumeDesc {
    const WORDS: usize = 13;

    fn parse(buf: &[u8], wi: usize) -> Self {
        let mut lv_id = [0u16; 5];
        for (i, w) in lv_id.iter_mut().enumerate() {
            *w = rdw(buf, wi + i);
        }
        SubVolumeDesc {
            lv_id,
            lv_size: rdlong(buf, wi + 5),
            lv_page: rdlong(buf, wi + 7),
            pv_page: rdlong(buf, wi + 9),
            n_pages: rdlong(buf, wi + 11),
        }
    }

    fn write(&self, buf: &mut [u8], wi: usize) {
        for (i, w) in self.lv_id.iter().enumerate() {
            wrw(buf, wi + i, *w);
        }
        wrlong(buf, wi + 5, self.lv_size);
        wrlong(buf, wi + 7, self.lv_page);
        wrlong(buf, wi + 9, self.pv_page);
        wrlong(buf, wi + 11, self.n_pages);
    }
}

// ---- physical volume root (page 0) ----

#[derive(Debug, Clone)]
pub struct PhysicalRoot {
    pub pv_id: [u16; 5],
    pub label: String,
    pub sub_volumes: Vec<SubVolumeDesc>,
}

impl PhysicalRoot {
    const SUBVOL_BASE: usize = 75; // word offset of the subVolumes array

    pub fn parse(data: &[u8]) -> Result<Self, FilesystemError> {
        if rdw(data, 0) != PR_SEAL {
            return Err(FilesystemError::Parse(format!(
                "Pilot: bad physical-root seal {:#o} (want {:#o})",
                rdw(data, 0),
                PR_SEAL
            )));
        }
        // No checksum gate: the root-page checksum word is never computed by
        // Pilot/Cedar (see `set_page_checksum`); the seal is the validity test.
        let label_len = rdw(data, 2) as usize;
        let mut pv_id = [0u16; 5];
        for (i, w) in pv_id.iter_mut().enumerate() {
            *w = rdw(data, 3 + i);
        }
        let count = (rdw(data, 64) as usize).min(MAX_SUBVOLS);
        let sub_volumes = (0..count)
            .map(|i| SubVolumeDesc::parse(data, Self::SUBVOL_BASE + i * SubVolumeDesc::WORDS))
            .collect();
        Ok(PhysicalRoot {
            pv_id,
            label: unpack_label(data, 44, label_len),
            sub_volumes,
        })
    }
}

// ---- logical volume root (logical page 0) ----

#[derive(Debug, Clone)]
pub struct LogicalRoot {
    pub v_id: [u16; 5],
    pub label: String,
    pub volume_type: u16,
    pub volume_size: u32,
    /// 32-bit `FileID` of the Cedar `client` directory file (`rootFile[client]`,
    /// `File.VolumeFile.client = 8`), or `None` when the slot is null (no name
    /// directory — names then come from leader pages / file IDs only). This file
    /// holds the FS name->FileID B-tree (`BTree` + `FSBackdoor.Entry`).
    pub client_fid: Option<u32>,
}

impl LogicalRoot {
    pub fn parse(data: &[u8]) -> Result<Self, FilesystemError> {
        if rdw(data, 0) != LR_SEAL {
            return Err(FilesystemError::Parse(format!(
                "Pilot: bad logical-root seal {:#o} (want {:#o})",
                rdw(data, 0),
                LR_SEAL
            )));
        }
        // No checksum gate (see PhysicalRoot::parse): seal is the validity test.
        let mut v_id = [0u16; 5];
        for (i, w) in v_id.iter_mut().enumerate() {
            *w = rdw(data, 2 + i);
        }
        let label_len = rdw(data, 7) as usize;
        // rootFile[client].fp.id (2 words) at word ROOTFILE_CLIENT_WORD; 0 = null.
        let client_fid = match rdlong(data, ROOTFILE_CLIENT_WORD) {
            0 => None,
            fid => Some(fid),
        };
        Ok(LogicalRoot {
            v_id,
            label: unpack_label(data, 8, label_len),
            volume_type: rdw(data, 28),
            volume_size: rdlong(data, 29),
            client_fid,
        })
    }
}

/// Summary of a parsed Pilot volume.
#[derive(Debug, Clone)]
pub struct PilotVolume {
    pub generation: Generation,
    pub pv_label: String,
    pub lv_label: String,
    pub volume_type: u16,
    pub volume_size: u32,
    /// Free logical pages, counted from page labels (the authoritative source).
    pub free_pages: u32,
    /// Free pages per the VAM bitmap (a hint), if a VAM file is present. Should
    /// agree with `free_pages`; a mismatch flags a VAM that needs rebuilding.
    pub vam_free_pages: Option<u32>,
    pub physical_root: PhysicalRoot,
    pub logical_root: LogicalRoot,
}

/// Parse and inspect a Pilot volume held in `disk` (PDI `fsFamily = 2`).
/// `generation` comes from the PDI `flags` bit 2.
pub fn read_volume(disk: &Disk, generation: Generation) -> Result<PilotVolume, FilesystemError> {
    if disk.geometry.family != FsFamily::Pilot {
        return Err(FilesystemError::Unsupported(
            "not a Pilot/Cedar disk (fsFamily != 2)".into(),
        ));
    }
    let page = |vda: usize| -> Result<&[u8], FilesystemError> {
        disk.sector(vda)
            .map(|s| s.data.as_slice())
            .ok_or_else(|| FilesystemError::Parse(format!("Pilot: page {vda} out of range")))
    };

    let physical_root = PhysicalRoot::parse(page(0)?)?;

    // Find the subvolume that holds the LV root (lv_page == rootPageNumber).
    let root_sv = physical_root
        .sub_volumes
        .iter()
        .find(|sv| sv.lv_page == ROOT_PAGE_NUMBER)
        .ok_or_else(|| {
            FilesystemError::Parse("Pilot: no subvolume contains the logical-volume root".into())
        })?;

    let lv_root_vda = root_sv.pv_page as usize; // logical page 0 -> physical pv_page
    let logical_root = LogicalRoot::parse(page(lv_root_vda)?)?;

    // Free space: scan the subvolume's logical pages and count freePage labels
    // (the page label is authoritative; the VAM is only a hint).
    let mut free_pages = 0u32;
    for lp in 0..root_sv.n_pages as usize {
        let vda = root_sv.pv_page as usize + lp;
        if let Some(s) = disk.sector(vda) {
            if is_free_label(&Label::parse(&s.label), logical_root.v_id, generation) {
                free_pages += 1;
            }
        }
    }

    let vam_free = vam_free_pages(disk, root_sv);

    Ok(PilotVolume {
        generation,
        pv_label: physical_root.label.clone(),
        lv_label: logical_root.label.clone(),
        volume_type: logical_root.volume_type,
        volume_size: logical_root.volume_size,
        free_pages,
        vam_free_pages: vam_free,
        physical_root,
        logical_root,
    })
}

/// Build a blank Pilot/Cedar volume on `geometry` (which must be `fsFamily =
/// Pilot`, 512-byte pages, 20-byte labels). Lays out: physical-volume root
/// (page 0), bad-page list (page 1), credentials (page 2), then a single
/// subvolume whose logical page 0 is the logical-volume root, the remaining
/// logical pages free, and a subvolume end marker as the last physical page.
///
/// Validation is by round-trip ([`read_volume`]); there is no period oracle.
pub fn create_blank(
    geometry: Geometry,
    generation: Generation,
    name: &str,
) -> Result<Disk, FilesystemError> {
    if geometry.family != FsFamily::Pilot
        || geometry.data_bytes as usize != PAGE_BYTES
        || geometry.label_bytes as usize != LABEL_BYTES
    {
        return Err(FilesystemError::Unsupported(
            "Pilot create_blank requires fsFamily=Pilot, 512-byte data, 20-byte labels".into(),
        ));
    }
    let total = geometry.total_sectors();
    // Need: 3 reserved + LV root + VAM (header + >=1 data) + >=1 free + marker.
    if total < OTHELLO_PV_RESERVE + 6 {
        return Err(FilesystemError::DiskFull(format!(
            "Pilot volume needs at least {} pages, got {total}",
            OTHELLO_PV_RESERVE + 6
        )));
    }

    // Deterministic non-null volume ids (a real installer would allocate UIDs).
    /* Volume IDs are absolute five-word UIDs.  They must not alias a Cedar
     * two-word FileID (the first boot file is FileID 2), because continuation
     * sectors carry freePage in word 7 and are distinguished from true free
     * pages by this ID. */
    let pv_id: [u16; 5] = [0x5044, 0x0001, 0, 0, 0]; // "PD"
    let lv_id: [u16; 5] = [0x4c56, 0x0001, 0, 0, 0]; // "LV"

    let marker_page = total - 1;
    let sv_pv_page = OTHELLO_PV_RESERVE as u32;
    let sv_n_pages = (marker_page - OTHELLO_PV_RESERVE) as u32; // logical pages [0..n)
    let volume_size = sv_n_pages;

    let sub = SubVolumeDesc {
        lv_id,
        lv_size: volume_size,
        lv_page: ROOT_PAGE_NUMBER,
        pv_page: sv_pv_page,
        n_pages: sv_n_pages,
    };

    let mut sectors: Vec<Sector> = (0..total)
        .map(|_| Sector::zeroed(LABEL_BYTES, PAGE_BYTES))
        .collect();

    // --- page 0: physical volume root ---
    {
        let s = &mut sectors[0];
        s.label = Label::new(pv_id, 0, attr::PHYSICAL_ROOT).bytes();
        let d = &mut s.data;
        wrw(d, 0, PR_SEAL);
        wrw(d, 1, PR_VERSION);
        wrw(d, 2, name.len().min(VOLUME_LABEL_LEN) as u16); // labelLength
        for (i, w) in pv_id.iter().enumerate() {
            wrw(d, 3 + i, *w);
        }
        pack_label(d, 44, name, VOLUME_LABEL_LEN / 2);
        wrw(d, 64, 1); // subVolumeCount
        sub.write(d, PhysicalRoot::SUBVOL_BASE);
        set_page_checksum(d);
    }

    // --- page 1: bad-page list (all null) ---
    sectors[1].label = Label::new(pv_id, 0, attr::BAD_PAGE_LIST).bytes();

    // --- page 2: credentials (blank) ---
    sectors[2].label = Label::new(pv_id, 0, attr::DATA).bytes();

    // --- logical page 0 (physical OTHELLO_PV_RESERVE): logical volume root ---
    {
        let s = &mut sectors[OTHELLO_PV_RESERVE];
        s.label = Label::new(lv_id, ROOT_PAGE_NUMBER, attr::LOGICAL_ROOT).bytes();
        let d = &mut s.data;
        wrw(d, 0, LR_SEAL);
        wrw(d, 1, LR_VERSION);
        for (i, w) in lv_id.iter().enumerate() {
            wrw(d, 2 + i, *w);
        }
        wrw(d, 7, name.len().min(VOLUME_LABEL_LEN) as u16); // labelLength
        pack_label(d, 8, name, VOLUME_LABEL_LEN / 2);
        wrw(d, 28, 3); // VolumeType::cedar
        wrlong(d, 29, volume_size); // volumeSize
                                    // rootFile array + fill left zero (VAM via label scan; see module doc)
        set_page_checksum(d);
    }

    // --- remaining logical pages: free ---
    for lp in 1..sv_n_pages as usize {
        let vda = OTHELLO_PV_RESERVE + lp;
        sectors[vda].label = Label::new(lv_id, lp as u32, attr::FREE_PAGE).bytes();
    }

    // --- subvolume end marker (last physical page) ---
    {
        let s = &mut sectors[marker_page];
        s.label = Label::new(pv_id, 0, attr::SUB_VOLUME_MARKER).bytes();
        let d = &mut s.data;
        wrw(d, 0, PSM_SEAL);
        wrw(d, 1, PSM_VERSION);
        for (i, w) in pv_id.iter().enumerate() {
            wrw(d, 2 + i, *w);
        }
        set_page_checksum(d);
    }

    let mut disk = Disk { geometry, sectors };
    // Install the VAM file (root-file slot 7) and fill its bitmap from labels.
    install_vam(&mut disk, &sub, generation);
    Ok(disk)
}

/// Install the Volume Allocation Map as a real file (`VAM_FILE_ID`) at logical
/// pages 1.. of the subvolume: a `header` page (run table) plus `data` pages
/// holding the `VAMObject` bitmap, with `rootFile[VAM]` and `lastFileID` set in
/// the LV root. The bitmap is then filled from the page labels.
fn install_vam(disk: &mut Disk, sv: &SubVolumeDesc, generation: Generation) {
    let pv = sv.pv_page as usize;
    let n_data = vam_data_pages(sv.lv_size);
    let header_lp = 1usize;
    let header_pages = 2usize; // run-table page + property page
    let data_lp0 = header_lp + header_pages;

    // Header page: a single run covering the VAM's data pages.
    {
        let s = &mut disk.sectors[pv + header_lp];
        s.label = Label::new(
            make_file_id(generation, 1, header_lp as u32),
            0,
            attr::HEADER,
        )
        .bytes();
        let d = &mut s.data;
        for b in d.iter_mut() {
            *b = 0;
        }
        wrw(d, 0, header_pages as u16);
        wrw(d, 1, RunTable::RUNS_PER_PAGE as u16); // table capacity
        wrlong(d, RunTable::RUNS_BASE, header_lp as u32);
        wrw(d, RunTable::RUNS_BASE + 2, (header_pages + n_data) as u16);
        wrlong(d, RunTable::RUNS_BASE + 3, LAST_LOGICAL_RUN);
    }
    // PilotDisk.mc clears header's low flag bits after the first sector in a
    // run, so the second property page carries freePage plus the same RelID.
    {
        let s = &mut disk.sectors[pv + header_lp + 1];
        s.label = Label::new(
            make_file_id(generation, 1, header_lp as u32),
            1,
            attr::FREE_PAGE,
        )
        .bytes();
        s.data.fill(0);
    }
    // Data pages (bitmap content written by rebuild_vam).
    for i in 0..n_data {
        let s = &mut disk.sectors[pv + data_lp0 + i];
        s.label = Label::new(
            make_file_id(generation, 1, header_lp as u32),
            i as u32,
            if i == 0 { attr::DATA } else { attr::FREE_PAGE },
        )
        .bytes();
        for b in s.data.iter_mut() {
            *b = 0;
        }
    }
    // LV root: point rootFile[VAM] at the VAM and reserve FileID 1.
    {
        let d = &mut disk.sectors[pv].data;
        wrw(d, ROOTFILE_VAM_WORD, VAM_FILE_ID[0]); // fp.id low
        wrw(d, ROOTFILE_VAM_WORD + 1, VAM_FILE_ID[1]); // fp.id high
                                                       // fp.da (File.FP.da, words +2..+3) = VAM leader's LOGICAL page. Cedar's
                                                       // File.Open -> DoOpen reads the file header starting at logicalRun
                                                       // [first: fp.da] (FileImpl.mesa DoOpen; File.DA == VolumeFormat.LogicalPage),
                                                       // so this MUST point at the VAM header (logical page `header_lp`). Leaving
                                                       // it 0 makes ReadVAM read logical page 0 (the LV root) as the VAM leader,
                                                       // corrupting the run table -> vamStatus inconsistent.
        wrlong(d, ROOTFILE_VAM_WORD + 2, header_lp as u32); // fp.da = VAM leader logical page
        wrlong(d, ROOTFILE_VAM_WORD + 4, header_lp as u32); // RootFile.page
        wrlong(d, 253, 1); // lastFileID = 1 (the VAM)
        set_page_checksum(d);
    }
    rebuild_vam(disk, sv);
}

/// Recompute the VAM bitmap from the authoritative page labels (scavenger
/// style): bit set = page in use (`attributes != freePage`), LSB-first within
/// each 16-bit word. A no-op if the VAM file isn't present.
fn rebuild_vam(disk: &mut Disk, sv: &SubVolumeDesc) {
    let pv = sv.pv_page as usize;
    let vsize = sv.lv_size as usize;

    // Locate the VAM header page and its data pages (run table).
    let mut header_lp = None;
    for lp in 1..sv.n_pages as usize {
        let l = Label::parse(&disk.sectors[pv + lp].label);
        if l.attributes == attr::HEADER && l.file_id[..2] == VAM_FILE_ID[..2] {
            header_lp = Some(lp);
            break;
        }
    }
    let Some(hlp) = header_lp else { return };
    let run = RunTable::parse(&disk.sectors[pv + hlp].data);
    let data_lps = run.data_logical_pages();

    // Build the bitmap words: rover(0..1)=0, size(2..3), then 1 bit per page.
    let volume_id = match LogicalRoot::parse(&disk.sectors[pv].data) {
        Ok(root) => root.v_id,
        Err(_) => return,
    };
    let mut words = vec![0u16; vam_data_words(sv.lv_size)];
    words[2] = (vsize & 0xffff) as u16;
    words[3] = (vsize >> 16) as u16;
    for lp in 0..vsize {
        let label = Label::parse(&disk.sectors[pv + lp].label);
        let in_use = !is_free_label(&label, volume_id, Generation::CedarNucleus);
        if in_use {
            words[4 + lp / 16] |= 1 << (lp % 16);
        }
    }

    // Scatter the words across the VAM's data pages.
    for (wi, &w) in words.iter().enumerate() {
        let page = wi / PAGE_WORDS;
        let off = wi % PAGE_WORDS;
        if let Some(&lp) = data_lps.get(page) {
            wrw(&mut disk.sectors[pv + lp].data, off, w);
        }
    }
}

/// Parse the VAM bitmap and count free pages (clear bits in `[0..volume_size)`),
/// or `None` if the volume has no VAM file. Used to cross-check the label scan.
fn vam_free_pages(disk: &Disk, sv: &SubVolumeDesc) -> Option<u32> {
    let pv = sv.pv_page as usize;
    let vsize = sv.lv_size as usize;
    let mut hlp = None;
    for lp in 1..sv.n_pages as usize {
        let l = Label::parse(&disk.sector(pv + lp)?.label);
        if l.attributes == attr::HEADER && l.file_id[..2] == VAM_FILE_ID[..2] {
            hlp = Some(lp);
            break;
        }
    }
    let run = RunTable::parse(&disk.sector(pv + hlp?)?.data);
    let data_lps = run.data_logical_pages();
    let word = |wi: usize| -> u16 {
        let page = wi / PAGE_WORDS;
        let off = wi % PAGE_WORDS;
        data_lps
            .get(page)
            .and_then(|&lp| disk.sector(pv + lp))
            .map(|s| rdw(&s.data, off))
            .unwrap_or(0)
    };
    let mut free = 0u32;
    for lp in 0..vsize {
        if word(4 + lp / 16) & (1 << (lp % 16)) == 0 {
            free += 1;
        }
    }
    Some(free)
}

// ---- file run table (in a file's header page) ----

/// `LogicalRunObject` end marker (`lastLogicalRun = LAST[INT]`).
const LAST_LOGICAL_RUN: u32 = 0x7fff_ffff;

/// A file's run table: `(firstLogicalPage, pageCount)` extents. Parsed from a
/// `header`-attribute page (`VolumeFormat.LogicalRunObject`).
#[derive(Debug, Clone, Default)]
pub struct RunTable {
    pub header_pages: u16,
    pub runs: Vec<(u32, u32)>,
}

impl RunTable {
    const RUNS_BASE: usize = 5; // word offset of the `runs` sequence
    const RUNS_PER_PAGE: usize = (PAGE_WORDS - Self::RUNS_BASE) / 3;

    fn parse(data: &[u8]) -> Self {
        let header_pages = rdw(data, 0);
        let mut runs = Vec::new();
        let mut wi = Self::RUNS_BASE;
        // Each LogicalRun is 3 words: first(LogicalPage, 2w) + size(1w).
        while wi + 3 < PAGE_WORDS {
            let first = rdlong(data, wi);
            if first == LAST_LOGICAL_RUN {
                break;
            }
            let size = rdw(data, wi + 2) as u32;
            if size == 0 {
                break;
            }
            runs.push((first, size));
            wi += 3;
        }
        RunTable { header_pages, runs }
    }

    /// Expand only the data portion of the file.  Runs describe the complete
    /// file, beginning with negative file pages for its header; data file page
    /// zero starts after `header_pages` logical pages have been skipped.
    fn data_logical_pages(&self) -> Vec<usize> {
        let mut file_page = -(self.header_pages as i64);
        let mut pages = Vec::new();
        for &(first, count) in &self.runs {
            for p in 0..count {
                if file_page >= 0 {
                    pages.push((first + p) as usize);
                }
                file_page += 1;
            }
        }
        pages
    }
}

/// Logical page number from a label, masking the classic page-0 flag bits
/// (`immutable`/`temporary`/`zeroSize`, word 6 bits 13-15; `filePageHi` is 7
/// bits). For the Cedar scheme the page number is small, so the mask is a no-op.
fn file_page_number(l: &Label) -> u32 {
    let lo = l.file_page & 0xffff;
    let hi = (l.file_page >> 16) & 0x7f;
    (hi << 16) | lo
}

/// Short label for a page `File.Type`/attribute, used in synthesized names.
fn type_label(a: u16) -> String {
    match a {
        10 => "temp".into(),
        11 => "txnState".into(),
        12 => "vmBacking".into(),
        15 => "anon".into(),
        16 => "txnLog".into(),
        attr::DATA => "data".into(),
        t if t >= 256 => "client".into(),
        t => format!("t{t}"),
    }
}

/// Trim a NUL-terminated run of printable ASCII out of `b`.
fn ascii_clean(b: &[u8]) -> String {
    b.iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as char)
        .filter(|&c| (' '..='~').contains(&c))
        .collect()
}

/// Try to read a human file name from a file's **leader page** (logical page 0).
/// The Pilot nucleus has no name directory, but files created through the
/// FileStream/FileTool carry their name in page 0. We handle the two leader
/// layouts seen on real Pilot 12.3 / XDE volumes (verified against the Dwarf
/// disks) plus the documented Cedar `fsLP`/`fullLP`, validating the result is a
/// sane printable string; returns `None` for raw nucleus files (no leader).
fn leader_name(data: &[u8]) -> Option<String> {
    if data.len() < 80 {
        return None;
    }
    let w = |wi: usize| be16(data, wi * 2);
    let validate = |s: String| -> Option<String> {
        let t = s.trim().to_string();
        (1..=72).contains(&t.len()).then_some(t)
    };
    // XDE / ViewPoint descriptive leader, of the form "(dir)name( date )":
    // word1 = length (chars), string at byte 4. word0 = 0x1061 stores it as
    // 8-bit ASCII (XDE); word0 = 0x1062 as 16-bit XCCS characters with the ASCII
    // in each word's low byte (ViewPoint). We keep "(dir)name" and drop the
    // trailing time-stamped group. (ViewPoint *client* files have no leader name
    // and no Pilot central directory — clientRootFile is 0 — so their names live
    // in the desktop / NS-Filing layer, not on this disk; they surface by ID.)
    if w(0) == 0x1061 || w(0) == 0x1062 {
        let n = (w(1) as usize).min(72);
        let s: String = if w(0) == 0x1061 {
            ascii_clean(&data[4..4 + n])
        } else {
            (0..n)
                .map(|i| data[(2 + i) * 2 + 1] as char)
                .take_while(|&c| c != '\0')
                .filter(|&c| (' '..='~').contains(&c))
                .collect()
        };
        // Drop a trailing "( <date> HH:MM:SS ... )" group (the last parenthesized
        // group containing a time colon).
        let s = match s.rfind('(') {
            Some(p) if s[p..].contains(':') => s[..p].trim_end().to_string(),
            _ => s,
        };
        return validate(s);
    }
    // XDE FileTool leader: word0 = 0x1b83, name length at byte 28, chars at 30.
    if w(0) == 0x1b83 {
        let n = data[28] as usize;
        if (1..=40).contains(&n) && 30 + n <= data.len() {
            return validate(ascii_clean(&data[30..30 + n]));
        }
    }
    // Cedar FileStream leader (fsLP): versionID 01240B @word0, nameLength @word10,
    // name (packed chars) @word11.
    if w(0) == 0o1240 {
        let n = (w(10) as usize).min(40);
        return validate(ascii_clean(&data[22..22 + n]));
    }
    // Cedar directory leader (fullLP): magic 25280 @word64, property list from
    // word65 (LPEntry{type,len,value}); file name = tFileName(6) StringBody.
    if w(64) == 25280 {
        let mut wi = 65usize;
        while wi + 4 < 250 {
            let ptype = w(wi);
            if ptype == 0xffff {
                break;
            }
            let plen = w(wi + 1) as usize;
            if ptype == 6 {
                let slen = (w(wi + 2) as usize).min(40);
                return validate(ascii_clean(&data[(wi + 4) * 2..(wi + 4) * 2 + slen]));
            }
            wi += plen + 2;
        }
    }
    None
}

/// One file discovered by a page-label scan: its identity, the pages that carry
/// it (absolute VDA, sorted by logical page), whether logical page 0 is a leader
/// page (name/metadata, excluded from content), and a display name.
struct PilotFile {
    name: String,
    #[allow(dead_code)]
    file_id: [u16; 5],
    /// `(logical page, absolute VDA)`, sorted by logical page.
    pages: Vec<(u32, usize)>,
    /// True if `pages[0]` is a leader page (logical page 0) to skip when reading.
    has_leader: bool,
    size: u64,
}

/// Read-only [`Filesystem`] view of a Pilot/Cedar volume.
///
/// Files are enumerated by a **page-label scan** (the scavenger's authoritative
/// source) across every subvolume, grouping data pages by `fileID`. This handles
/// both the Cedar-nucleus scheme (our own writer: `header` run-table + `data`
/// pages) and the classic Pilot scheme (e.g. 6085 Pilot 12.3: pages carry the
/// file's `File.Type`), and reconstructs fragmented files from the labels alone.
/// Files surface by file ID; the nucleus has no name directory, so names are
/// synthesized (the PFS name layer is a separate, future concern).
pub struct PilotFilesystem {
    disk: Disk,
    #[allow(dead_code)]
    generation: Generation,
    volume: PilotVolume,
    files: Vec<PilotFile>,
}

impl PilotFilesystem {
    pub fn open(disk: Disk, generation: Generation) -> Result<Self, FilesystemError> {
        let volume = read_volume(&disk, generation)?;
        let mut files = build_files(&disk, &volume);
        // Replace synthetic / leader names with the real names from the Cedar
        // client name directory (rootFile[client] FS B-tree), when present.
        apply_client_names(&disk, &volume, &mut files);
        Ok(Self {
            disk,
            generation,
            volume,
            files,
        })
    }

    fn enumerate(&self) -> Vec<FileEntry> {
        self.files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                FileEntry::new_file(f.name.clone(), format!("/{}", f.name), f.size, i as u64)
            })
            .collect()
    }

    /// Cedar-nucleus file IDs paired with their current display names.  This
    /// is intentionally a narrow maintenance API for volume trimming tools;
    /// callers can retain boot files while releasing ordinary cached content.
    pub fn file_ids(&self) -> Vec<(String, u32)> {
        self.files
            .iter()
            .filter_map(|f| {
                (self.generation == Generation::CedarNucleus && f.file_id[4] == 0).then_some((
                    f.name.clone(),
                    u32::from(f.file_id[0]) | (u32::from(f.file_id[1]) << 16),
                ))
            })
            .collect()
    }
}

/// Scan every subvolume's page labels and group user-file data pages by `fileID`
/// into a sorted, deduplicated file table.
fn build_files(disk: &Disk, volume: &PilotVolume) -> Vec<PilotFile> {
    if volume.generation == Generation::CedarNucleus {
        let mut files = Vec::new();
        for (svidx, sv) in volume.physical_root.sub_volumes.iter().enumerate() {
            let pv = sv.pv_page as usize;
            for lp in 1..sv.n_pages as usize {
                let Some(sector) = disk.sector(pv + lp) else {
                    continue;
                };
                let label = Label::parse(&sector.label);
                if label.attributes != attr::HEADER || label.file_id[..2] == VAM_FILE_ID[..2] {
                    continue;
                }
                let run = RunTable::parse(&sector.data);
                let pages: Vec<(u32, usize)> = run
                    .data_logical_pages()
                    .into_iter()
                    .enumerate()
                    .filter_map(|(file_page, logical)| {
                        (logical < sv.n_pages as usize).then_some((file_page as u32, pv + logical))
                    })
                    .collect();
                if pages.is_empty() {
                    continue;
                }
                files.push(PilotFile {
                    name: format!(
                        "LV{svidx}_{:04X}{:04X}_data",
                        label.file_id[0], label.file_id[1]
                    ),
                    file_id: label.file_id,
                    size: pages.len() as u64 * PAGE_BYTES as u64,
                    pages,
                    has_leader: false,
                });
            }
        }
        files.sort_by(|a, b| a.name.cmp(&b.name));
        dedup_names(&mut files);
        return files;
    }

    // Per file ID: its type word plus its `(logical page, absolute VDA)` pages.
    type FileGroup = (u16, Vec<(u32, usize)>);
    let mut files: Vec<PilotFile> = Vec::new();
    for (svidx, sv) in volume.physical_root.sub_volumes.iter().enumerate() {
        let pv = sv.pv_page as usize;
        let mut by_id: HashMap<[u16; 5], FileGroup> = HashMap::new();
        for lp in 0..sv.n_pages as usize {
            let vda = pv + lp;
            let Some(s) = disk.sector(vda) else { continue };
            let l = Label::parse(&s.label);
            if is_free_label(&l, volume.logical_root.v_id, volume.generation)
                || matches!(l.attributes, 1..=5)
                || l.file_id[..2] == VAM_FILE_ID[..2]
                || l.file_id == [0; 5]
                || l.file_id == [0xffff; 5]
            {
                continue;
            }
            let entry = by_id.entry(l.file_id).or_insert((l.attributes, Vec::new()));
            entry.1.push((file_page_number(&l), vda));
        }
        for (id, (type_word, mut pages)) in by_id {
            pages.sort_by_key(|&(fp, _)| fp);
            // Logical page 0, if present, may be a leader page carrying the file
            // name (and is then metadata, not content).
            let synthetic = format!(
                "LV{svidx}_{:04X}{:04X}_{}",
                id[0],
                id[1],
                type_label(type_word)
            );
            let (name, has_leader) = match pages.first() {
                Some(&(0, vda)) => match disk.sector(vda).and_then(|s| leader_name(&s.data)) {
                    Some(n) => (n, true),
                    None => (synthetic, false),
                },
                _ => (synthetic, false),
            };
            let content_pages = pages.len() - has_leader as usize;
            files.push(PilotFile {
                name,
                file_id: id,
                pages,
                has_leader,
                size: content_pages as u64 * PAGE_BYTES as u64,
            });
        }
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    dedup_names(&mut files);
    files
}

/// Make display names unique by appending " (N)" to repeats (preserves the
/// first occurrence). Browse views key navigation on the path, so duplicate
/// names would otherwise collide.
fn dedup_names(files: &mut [PilotFile]) {
    let mut seen: HashMap<String, u32> = HashMap::new();
    for f in files.iter_mut() {
        let count = seen.entry(f.name.clone()).or_insert(0);
        if *count > 0 {
            f.name = format!("{} ({})", f.name, *count + 1);
        }
        *count += 1;
    }
}

impl Filesystem for PilotFilesystem {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        Ok(self.enumerate())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let file = self
            .files
            .get(entry.location as usize)
            .ok_or_else(|| FilesystemError::Parse("Pilot: bad file index".into()))?;
        // The label scan already ordered the data pages by logical page and
        // resolved fragmentation, so concatenating them yields the file. Skip a
        // leader page (logical page 0) — it holds the name/metadata, not content.
        let mut out = Vec::new();
        let skip = file.has_leader as usize;
        for &(_logical, vda) in file.pages.iter().skip(skip) {
            if out.len() >= max_bytes {
                break;
            }
            let Some(sector) = self.disk.sector(vda) else {
                break;
            };
            let take = (max_bytes - out.len()).min(PAGE_BYTES);
            out.extend_from_slice(&sector.data[..take]);
        }
        Ok(out)
    }

    fn volume_label(&self) -> Option<&str> {
        Some(&self.volume.lv_label)
    }

    fn fs_type(&self) -> &str {
        "Pilot/Cedar"
    }

    fn total_size(&self) -> u64 {
        self.disk.geometry.total_sectors() as u64 * self.disk.geometry.data_bytes as u64
    }

    fn used_size(&self) -> u64 {
        let used_pages = self
            .volume
            .volume_size
            .saturating_sub(self.volume.free_pages) as u64;
        used_pages * PAGE_BYTES as u64
    }
}

/// Append a file to a Pilot volume (Cedar nucleus generation), returning the
/// new disk and the allocated 32-bit `FileID`. A normal Cedar file has two
/// header pages (one run-table page plus one property page), followed by
/// `ceil(len/512)` data pages. `lastFileID` in the LV root is advanced. Pages
/// are page-granular (the byte length belongs in file properties), so a
/// read-back returns data zero-padded to the next page boundary.
///
/// Allocation marks page labels (`freePage` -> `header`/`data`), which is the
/// authoritative free/used state; a VAM-file rebuild is a separate refinement.
pub fn add_file(
    disk: &Disk,
    generation: Generation,
    data: &[u8],
) -> Result<(Disk, u32), FilesystemError> {
    let vol = read_volume(disk, generation)?;
    let sv = vol
        .physical_root
        .sub_volumes
        .iter()
        .find(|s| s.lv_page == ROOT_PAGE_NUMBER)
        .cloned()
        .ok_or_else(|| FilesystemError::Parse("Pilot: no root subvolume".into()))?;

    let n_data = data.len().div_ceil(PAGE_BYTES).max(1);
    const HEADER_PAGES: usize = 2;
    let need = HEADER_PAGES + n_data;

    // Collect free logical pages (logical 0 is the LV root; the marker lives
    // past the subvolume, so [1..n_pages) is fair game). First-fit, allowing a
    // non-contiguous data region (the run table records multiple extents).
    let n_pages = sv.n_pages as usize;
    let pv_page = sv.pv_page as usize;
    let free: Vec<u32> = (1..n_pages)
        .filter(|&lp| {
            disk.sector(pv_page + lp)
                .map(|s| is_free_label(&Label::parse(&s.label), vol.logical_root.v_id, generation))
                .unwrap_or(false)
        })
        .map(|lp| lp as u32)
        .collect();
    if free.len() < need {
        return Err(FilesystemError::DiskFull(format!(
            "Pilot: need {need} free pages for a {}-byte file, have {}",
            data.len(),
            free.len()
        )));
    }

    let header_at = free
        .windows(HEADER_PAGES)
        .position(|w| w[1] == w[0] + 1)
        .ok_or_else(|| {
            FilesystemError::DiskFull(
                "Pilot: no adjacent pair of pages for a normal file header".into(),
            )
        })?;
    let header_lps = [free[header_at], free[header_at + 1]];
    let data_lps: Vec<u32> = free
        .iter()
        .copied()
        .filter(|lp| !header_lps.contains(lp))
        .take(n_data)
        .collect();
    // Runs describe the complete file in file-page order: both (negative)
    // header pages first, then data page zero onward.
    let allocated_lps = header_lps.into_iter().chain(data_lps.iter().copied());
    let mut runs: Vec<(u32, u32)> = Vec::new();
    for lp in allocated_lps {
        match runs.last_mut() {
            Some((first, len)) if *first + *len == lp => *len += 1,
            _ => runs.push((lp, 1)),
        }
    }
    if runs.len() + 1 > RunTable::RUNS_PER_PAGE {
        return Err(FilesystemError::DiskFull(
            "Pilot: file too fragmented for a single-page run table".into(),
        ));
    }

    let mut disk = disk.clone();

    // Allocate a FileID (advance the LV-root counter at word 253).
    let lv_root_vda = pv_page; // logical page 0
    let fid = rdlong(&disk.sectors[lv_root_vda].data, 253).wrapping_add(1);
    let file_id = make_file_id(generation, fid, header_lps[0]);

    // First header page: run table describing headers and data extents.
    {
        let s = &mut disk.sectors[pv_page + header_lps[0] as usize];
        s.label = Label::new(file_id, 0, attr::HEADER).bytes();
        let d = &mut s.data;
        for b in d.iter_mut() {
            *b = 0;
        }
        wrw(d, 0, HEADER_PAGES as u16);
        wrw(d, 1, RunTable::RUNS_PER_PAGE as u16); // table capacity
        let mut wi = RunTable::RUNS_BASE;
        for &(first, len) in &runs {
            wrlong(d, wi, first); // LogicalRun.first
            wrw(d, wi + 2, len as u16); // LogicalRun.size
            wi += 3;
        }
        wrlong(d, wi, LAST_LOGICAL_RUN); // terminator
    }

    // Second header page: property storage. It must carry Cedar's validation
    // seal, or every FS property accessor rejects the file and directory
    // enumeration silently skips it. `bytes` is the payload length Cedar
    // reports in a listing; `created` is left null (Cedar prints "??") and the
    // name body empty, since neither is known at this level -- the name is
    // applied later through the client directory.
    {
        let s = &mut disk.sectors[pv_page + header_lps[1] as usize];
        s.label = Label::new(file_id, 1, attr::FREE_PAGE).bytes();
        s.data.fill(0);
        let d = &mut s.data;
        for (i, w) in PROPERTIES_VALIDATION.iter().enumerate() {
            wrw(d, i, *w);
        }
        wrlong(d, PROP_BYTES_WORD, data.len() as u32);
        wrw(d, PROP_KEEP_WORD, 0);
        wrw(d, PROP_VERSION_WORD, 1);
        wrw(d, PROP_NAME_WORD, 0); // empty nameBody TextRep
    }

    // Data pages (filePage numbered 0.. within the file).
    for (i, &lp) in data_lps.iter().enumerate() {
        let s = &mut disk.sectors[pv_page + lp as usize];
        // PilotDisk.mc/KSectorDone clears the low file-flag bits in its
        // reusable client label after the first successful sector.  The
        // following operation therefore presents freePage Attributes even
        // across a physical run boundary; the RelID and filePage identify
        // the continuation sector.  A non-volume RelID keeps these pages
        // allocated (is_free_label deliberately checks both fields).
        s.label = Label::new(
            file_id,
            i as u32,
            if i == 0 { attr::DATA } else { attr::FREE_PAGE },
        )
        .bytes();
        let off = i * PAGE_BYTES;
        let end = (off + PAGE_BYTES).min(data.len());
        let chunk = &data[off..end];
        for b in s.data.iter_mut() {
            *b = 0;
        }
        s.data[..chunk.len()].copy_from_slice(chunk);
    }

    // Persist the advanced FileID and re-checksum the LV root.
    wrlong(&mut disk.sectors[lv_root_vda].data, 253, fid);
    set_page_checksum(&mut disk.sectors[lv_root_vda].data);

    rebuild_vam(&mut disk, &sv);
    Ok((disk, fid))
}

/// Delete the file with 32-bit `FileID` `fid` (Cedar nucleus), freeing every
/// page whose label carries that file (header + data), i.e. returning them to
/// `freePage`. The LV-root `lastFileID` is left unchanged (ids are not reused).
pub fn delete_file(disk: &Disk, fid: u32) -> Result<Disk, FilesystemError> {
    let vol = read_volume(disk, Generation::CedarNucleus)?;
    let sv = vol
        .physical_root
        .sub_volumes
        .iter()
        .find(|s| s.lv_page == ROOT_PAGE_NUMBER)
        .cloned()
        .ok_or_else(|| FilesystemError::Parse("Pilot: no root subvolume".into()))?;

    let target = [(fid & 0xffff) as u16, (fid >> 16) as u16];
    let pv_page = sv.pv_page as usize;
    let mut disk = disk.clone();
    let mut freed = 0u32;
    for lp in 1..sv.n_pages as usize {
        let vda = pv_page + lp;
        let label = Label::parse(&disk.sectors[vda].label);
        let is_file_page = !is_free_label(&label, vol.logical_root.v_id, Generation::CedarNucleus)
            && !matches!(label.attributes, 1..=5);
        if is_file_page && label.file_id[0] == target[0] && label.file_id[1] == target[1] {
            disk.sectors[vda].label =
                Label::new(vol.logical_root.v_id, lp as u32, attr::FREE_PAGE).bytes();
            for b in disk.sectors[vda].data.iter_mut() {
                *b = 0;
            }
            freed += 1;
        }
    }
    if freed == 0 {
        return Err(FilesystemError::NotFound(format!("Pilot: no file {fid}")));
    }
    rebuild_vam(&mut disk, &sv);
    Ok(disk)
}

// ---- Cedar FS name directory (rootFile[client] B-tree) ----
//
// Cedar's nucleus has no name directory; the human name -> FileID map lives in a
// separate "client" file (`rootFile[client]`, `File.VolumeFile.client = 8`),
// stored as an FS B-tree. On-disk format (cedar6.1 sources under
// `~/PARC-Stuff/cyan/cedar6.1/{btree,fs,file}`):
//
//   - A BTree page = 4 Pilot pages = 1024 words (`BTreeVM` filePagesPerPage = 4,
//     `FS.WordsForPages[4]`); file page 0 of the client file is the *statePage*.
//   - statePage (`BTreeInternal.TreeState`): seal(0) = 046273B, pageSize(1, in
//     words), rootPage(2), greatestPage(3), firstFreePage(4), entryCount(5, LONG),
//     depth(7).
//   - tree page (`BTreeInternal.BTreePage`): freeWords(0), minPage(1), then
//     packed `BTreeEntry`s from word 2. A free page has freeWords = LAST[CARDINAL].
//   - `BTreeEntry` = grPage(1 word, the greater-than child) + ClientEntry.
//   - ClientEntry = `FSBackdoor.Entry`: size(0, whole-entry words incl. trailing
//     text), version(1), nameBody(2, a TextRP = word-relative pointer to a
//     TextRep), type(3: 0 = local, 1 = attached, 2 = cached), and for `local`:
//     keep(4), fp(5: `File.FP`).
//   - `File.FP` = id(0: FileID[2]) + da(2: DA[2]); the 32-bit Cedar FileID is
//     `fp.id`, equal to the nucleus label fileID's first two words.
//   - TextRep = PACKED SEQUENCE length: CARDINAL OF CHAR — word 0 = char count,
//     then 2 chars/word with char[0] in the high byte (so the packed chars read
//     as a natural left-to-right byte run on a big-endian page).
//
// We read the directory to give files their real names, and write a minimal
// (single root-leaf) tree so a created volume carries navigable names and the
// reader is round-trip-testable (no period disk has a populated client
// directory to validate against — every available real Pilot pack is classic
// ViewPoint/XDE with clientRootFile = 0).

/// `BTreeInternal.sealValue`.
const BTREE_SEAL: u16 = 0o46273;
/// BTree page size in words (FS opens the directory at 4 Pilot pages / BTree page).
const BTREE_PAGE_WORDS: usize = 4 * PAGE_WORDS; // 1024
const BTREE_PAGE_BYTES: usize = BTREE_PAGE_WORDS * 2;
/// `BTreeInternal.nilPage` (== statePage); a leaf's grPage / minPage links.
const BTREE_NIL_PAGE: u16 = 0;
/// `freeWords` value marking a page as free (`BTreeInternal.freePageMarker`).
const BTREE_FREE_MARKER: u16 = 0xffff;
/// `FSBackdoor.EntryType.local`.
const FS_ENTRY_LOCAL: u16 = 0;
/// Word offset of the `local` entry's TextRep (after size, version, nameBody,
/// type, keep, fp = id(2) + da(2) = words 0..8), so nameBody's relative pointer.
const FS_LOCAL_TEXT_WORD: usize = 9;
/// Keep expandable pages behind the initial client-directory leaf.  Cedar
/// creates local names for cached remote files; a tree with firstFreePage=NIL
/// reports "No more free names" even when the Pilot volume itself has space.
const BTREE_RESERVED_FREE_PAGES: u16 = 4;
/// FSFileOpsImpl opens the client directory/cache B-tree with four Pilot
/// pages per B-tree page, 32 buffers, and `initialPages = 4 * 32`.
const BTREE_FILE_PAGES_PER_BUFFER: usize = 4;
const BTREE_INITIAL_BUFFERS: usize = 32;
const BTREE_INITIAL_FILE_PAGES: usize = BTREE_FILE_PAGES_PER_BUFFER * BTREE_INITIAL_BUFFERS;

/// One decoded name-directory entry: a human name, its FName version, and the
/// 32-bit Cedar `FileID` it maps to.
#[derive(Debug, Clone)]
struct ClientDirEntry {
    name: String,
    version: u16,
    fid: u32,
    /// `File.FP.da` — the LOGICAL page number of the file's header within the
    /// volume, which is what Cedar dereferences: `FileImpl.Open` builds
    /// `VolumeFormat.LogicalRun[first: fp.da, size: headerPages]` and hands it
    /// to `TranslateLogicalRun`. Leaving it 0 points every entry at logical
    /// page 0 (the volume root), and Cedar rejects the enumeration with
    /// `FS.Error $badFP` — "File.FP from directory/cache doesn't correspond to
    /// a local volume" (`FSMainImpl2.mesa`).
    da: u32,
}

/// Logical page of a file's run-table leader (its first header page), i.e. the
/// value Cedar wants in `File.FP.da`. Scans the subvolume for the HEADER page
/// whose label carries this FileID at file-page 0 — the same identification
/// `add_file` uses when it stamps that label.
fn header_lp_for(disk: &Disk, sv: &SubVolumeDesc, fid: u32) -> Option<u32> {
    (1..sv.n_pages).find(|&lp| {
        disk.sector(sv.pv_page as usize + lp as usize)
            .map(|s| {
                let label = Label::parse(&s.label);
                label.attributes == attr::HEADER
                    && label.file_id[0] == (fid & 0xffff) as u16
                    && label.file_id[1] == (fid >> 16) as u16
                    && file_page_number(&label) == 0
            })
            .unwrap_or(false)
    })
}

/// Read a `TextRep` (PACKED SEQUENCE length: CARDINAL OF CHAR) at word `wi` in a
/// BTree page: word 0 = char count, chars follow 2/word (char[0] in the high
/// byte). Returns the printable-ASCII name, or `None` if out of range / empty.
fn read_textrep(page: &[u8], wi: usize, page_words: usize) -> Option<String> {
    if wi >= page_words {
        return None;
    }
    let len = rdw(page, wi) as usize;
    if len == 0 || len > 256 {
        return None;
    }
    let start = (wi + 1) * 2; // chars begin at the next word; bytes are in order
    if start + len > page.len() {
        return None;
    }
    let s: String = page[start..start + len]
        .iter()
        .map(|&b| b as char)
        .filter(|&c| (' '..='~').contains(&c))
        .collect();
    (!s.is_empty()).then_some(s)
}

/// Parse the FS name -> FileID B-tree out of the `client` file's concatenated
/// data pages. Best-effort and panic-free (this reads untrusted on-disk data):
/// malformed pages/entries are skipped. Returns the `local` entries. Walks every
/// tree page scavenger-style — a B-tree holds each entry exactly once across all
/// nodes, so a full scan enumerates the directory without following child links.
fn parse_client_directory(client_data: &[u8]) -> Vec<ClientDirEntry> {
    let mut out = Vec::new();
    if client_data.len() < BTREE_PAGE_BYTES || rdw(client_data, 0) != BTREE_SEAL {
        return out; // missing / wrong statePage
    }
    let page_words = match rdw(client_data, 1) as usize {
        w if (4..=BTREE_PAGE_WORDS).contains(&w) => w,
        _ => BTREE_PAGE_WORDS,
    };
    let page_bytes = page_words * 2;
    let n_pages = client_data.len() / page_bytes;
    for p in 1..n_pages {
        let base = p * page_bytes;
        let page = &client_data[base..base + page_bytes];
        let free_words = rdw(page, 0) as usize;
        if free_words == BTREE_FREE_MARKER as usize || free_words >= page_words {
            continue; // free page or implausible
        }
        // Entries occupy words [2, page_words - free_words), packed.
        let used_end = page_words - free_words;
        let mut w = 2usize; // first entry's grPage word
        while w + 2 <= used_end {
            let entry_w = w + 1; // Entry follows the 1-word grPage
            let size = rdw(page, entry_w) as usize; // whole-entry words
            if size == 0 || entry_w + size > page_words {
                break; // corrupt; abandon this page
            }
            if rdw(page, entry_w + 3) == FS_ENTRY_LOCAL && size > FS_LOCAL_TEXT_WORD {
                let version = rdw(page, entry_w + 1);
                let name_rp = rdw(page, entry_w + 2) as usize; // word-relative ptr
                let fid = rdlong(page, entry_w + 5); // fp.id (FileID, 2 words)
                let da = rdlong(page, entry_w + 7); // fp.da (DA, 2 words)
                if let Some(name) = read_textrep(page, entry_w + name_rp, page_words) {
                    out.push(ClientDirEntry {
                        name,
                        version,
                        fid,
                        da,
                    });
                }
            }
            w = entry_w + size; // advance past grPage + entry
        }
    }
    out
}

/// Build the `client` name-directory B-tree as a flat byte buffer — a statePage
/// (BTree page 0) plus a single root *leaf* page (BTree page 1) — from `entries`,
/// sorted in `FSDir.Compare` order (case-insensitive name, then version). Errors
/// if the entries don't fit one leaf page (multi-page trees are a future
/// extension; one 1024-word page holds ~50 typical names).
fn build_client_directory(entries: &[ClientDirEntry]) -> Result<Vec<u8>, FilesystemError> {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|a, b| {
        a.name
            .to_ascii_uppercase()
            .cmp(&b.name.to_ascii_uppercase())
            .then(a.version.cmp(&b.version))
    });

    // Serialize one entry's ClientEntry words (everything AFTER the 1-word
    // grPage): [size, version, nameBodyRelPtr, type, keep, fid(2), da(2),
    // nameLen, name chars packed 2/word, char[0] in the high byte]. Identical
    // bytes to the single-leaf writer, just addressable so it can go in any
    // page (leaf or the promoted separators in an internal node).
    fn entry_words(e: &ClientDirEntry) -> Vec<u16> {
        let name = e.name.as_bytes();
        let text_words = 1 + name.len().div_ceil(2);
        let size = FS_LOCAL_TEXT_WORD + text_words;
        let mut v = vec![0u16; size];
        v[0] = size as u16;
        v[1] = e.version;
        v[2] = FS_LOCAL_TEXT_WORD as u16;
        v[3] = FS_ENTRY_LOCAL;
        v[4] = 0; // keep
        v[5] = (e.fid & 0xffff) as u16;
        v[6] = (e.fid >> 16) as u16;
        v[7] = (e.da & 0xffff) as u16;
        v[8] = (e.da >> 16) as u16;
        v[FS_LOCAL_TEXT_WORD] = name.len() as u16;
        for (i, &b) in name.iter().enumerate() {
            let wi = FS_LOCAL_TEXT_WORD + 1 + i / 2;
            if i & 1 == 0 {
                v[wi] |= (b as u16) << 8;
            } else {
                v[wi] |= b as u16;
            }
        }
        v
    }

    // Lay a page: word 0 freeWords, word 1 minPage, then (grPage, entry) items.
    fn write_node(min_page: u16, items: &[(u16, &[u16])]) -> Vec<u8> {
        let mut page = vec![0u8; BTREE_PAGE_BYTES];
        wrw(&mut page, 1, min_page);
        let mut w = 2usize;
        for (gr, words) in items {
            wrw(&mut page, w, *gr);
            for (k, word) in words.iter().enumerate() {
                wrw(&mut page, w + 1 + k, *word);
            }
            w += 1 + words.len();
        }
        wrw(&mut page, 0, (BTREE_PAGE_WORDS - w) as u16);
        page
    }

    let words: Vec<Vec<u16>> = sorted.iter().map(entry_words).collect();

    // Greedily pack entries into leaves; the entry that overflows a full leaf
    // is PROMOTED as the separator between it and the next leaf (a B-tree keeps
    // each entry exactly once — separators live in the internal node, not in a
    // leaf), and the next leaf starts after it.
    const LEAF_CAP: usize = BTREE_PAGE_WORDS - 2; // minus freeWords, minPage
    let mut leaves: Vec<Vec<usize>> = vec![Vec::new()];
    let mut separators: Vec<usize> = Vec::new();
    let mut used = 0usize;
    for (i, w) in words.iter().enumerate() {
        let slot = 1 + w.len(); // grPage + entry
        if slot > LEAF_CAP {
            return Err(FilesystemError::DiskFull(format!(
                "Pilot: name {:?} is too long for one B-tree page",
                sorted[i].name
            )));
        }
        if used + slot > LEAF_CAP && !leaves.last().unwrap().is_empty() {
            separators.push(i);
            leaves.push(Vec::new());
            used = 0;
        } else {
            leaves.last_mut().unwrap().push(i);
            used += slot;
        }
    }

    let n_leaves = leaves.len();
    let mut state = vec![0u8; BTREE_PAGE_BYTES];
    wrw(&mut state, 0, BTREE_SEAL);
    wrw(&mut state, 1, BTREE_PAGE_WORDS as u16);
    wrlong(&mut state, 5, sorted.len() as u32);

    // Leaf pages: page numbers 1..=n_leaves. Every leaf entry has grPage = nil.
    let nil = BTREE_NIL_PAGE;
    let mut leaf_pages: Vec<Vec<u8>> = Vec::with_capacity(n_leaves);
    for leaf in &leaves {
        let items: Vec<(u16, &[u16])> = leaf.iter().map(|&i| (nil, words[i].as_slice())).collect();
        leaf_pages.push(write_node(nil, &items));
    }

    let (root_page, reserved_start, depth): (u16, u16, u16);
    let root: Option<Vec<u8>>;
    if n_leaves == 1 {
        // Single leaf is the root (depth 1), as before.
        root_page = 1;
        reserved_start = 2;
        depth = 1;
        root = None;
    } else {
        // Internal root at page n_leaves+1: minPage = leaf 1 (page 1), then one
        // separator per gap with grPage pointing at the leaf to its right.
        let items: Vec<(u16, &[u16])> = separators
            .iter()
            .enumerate()
            .map(|(k, &i)| ((k as u16) + 2, words[i].as_slice()))
            .collect();
        let root_words: usize = 2 + items.iter().map(|(_, w)| 1 + w.len()).sum::<usize>();
        if root_words > BTREE_PAGE_WORDS {
            return Err(FilesystemError::DiskFull(format!(
                "Pilot: {} names need a 3-level B-tree (only 2 levels written)",
                sorted.len()
            )));
        }
        root_page = n_leaves as u16 + 1;
        reserved_start = n_leaves as u16 + 2;
        depth = 2;
        root = Some(write_node(1, &items));
    }
    wrw(&mut state, 2, root_page);
    wrw(
        &mut state,
        3,
        reserved_start - 1 + BTREE_RESERVED_FREE_PAGES,
    );
    wrw(&mut state, 4, reserved_start);
    wrw(&mut state, 7, depth);

    let mut out = state;
    for leaf in &leaf_pages {
        out.extend_from_slice(leaf);
    }
    if let Some(root) = &root {
        out.extend_from_slice(root);
    }
    // Free B-tree pages, chained through minPage (Cedar unlinks one when the
    // local cache needs the directory to grow).
    let last_free = reserved_start + BTREE_RESERVED_FREE_PAGES - 1;
    for page in reserved_start..=last_free {
        let mut free = vec![0u8; BTREE_PAGE_BYTES];
        wrw(&mut free, 0, BTREE_FREE_MARKER);
        wrw(
            &mut free,
            1,
            if page == last_free {
                BTREE_NIL_PAGE
            } else {
                page + 1
            },
        );
        out.extend_from_slice(&free);
    }
    // FSFileOpsImpl creates its client directory/cache backing file at this
    // fixed size; the implicit zero tail is real (unused cache) file storage.
    out.resize(BTREE_INITIAL_FILE_PAGES * PAGE_BYTES, 0);
    Ok(out)
}

/// PilotDisk.mc clears `Lab.fileFlags` after each completed sector.  Cedar's
/// FS B-tree transfers its backing file in `bufferSize = 4` page operations
/// (FSFileOpsImpl), so the first label in every four-page buffer has the
/// `data` flag and its three continuation labels have `freePage` flags.  The
/// non-volume RelID still makes those continuation pages allocated.
fn set_cedar_btree_label_flags(disk: &mut Disk, file_id: [u16; 5]) {
    for sector in &mut disk.sectors {
        let mut label = Label::parse(&sector.label);
        if label.file_id != file_id || label.attributes == attr::HEADER {
            continue;
        }
        let file_page = file_page_number(&label) as usize;
        label.attributes = if file_page.is_multiple_of(BTREE_FILE_PAGES_PER_BUFFER) {
            attr::DATA
        } else {
            attr::FREE_PAGE
        };
        sector.label = label.bytes();
    }
}

/// Install (or replace) the Cedar `client` name directory: build the FS B-tree
/// from `entries` (name, version, 32-bit FileID), store it as a nucleus file via
/// [`add_file`], and point `rootFile[client]` at it. `entries` should reference
/// FileIDs already present on the volume (e.g. from prior [`add_file`] calls).
/// Returns the updated disk.
///
/// Cedar opens a root file through `fp.da`, so both disk-address hints must
/// identify the directory file's run-table leader.  A zero hint happens to
/// round-trip through our label-scanning reader, but makes Cedar interpret the
/// logical-volume root (page 0) as the client file's leader.
pub fn set_client_directory(
    disk: &Disk,
    generation: Generation,
    entries: &[(String, u16, u32)],
) -> Result<Disk, FilesystemError> {
    // Resolve each entry's File.FP.da (the logical page of its header) BEFORE
    // the B-tree becomes a file itself, so the lookups see exactly the files
    // being named. An entry whose file cannot be found is dropped rather than
    // written with a null da: Cedar raises $badFP on the first such entry and
    // abandons the rest of the enumeration, so one bad entry would cost every
    // later one.
    let vol_now = read_volume(disk, generation)?;
    let sv_now = vol_now
        .physical_root
        .sub_volumes
        .iter()
        .find(|s| s.lv_page == ROOT_PAGE_NUMBER)
        .cloned()
        .ok_or_else(|| FilesystemError::Parse("Pilot: no root subvolume".into()))?;
    let dir_entries: Vec<ClientDirEntry> = entries
        .iter()
        .filter_map(|(name, version, fid)| {
            header_lp_for(disk, &sv_now, *fid).map(|da| ClientDirEntry {
                name: name.clone(),
                version: *version,
                fid: *fid,
                da,
            })
        })
        .collect();
    let btree = build_client_directory(&dir_entries)?;
    let (mut disk, client_fid) = add_file(disk, generation, &btree)?;

    if generation == Generation::CedarNucleus {
        let vol = read_volume(&disk, generation)?;
        let sv = vol
            .physical_root
            .sub_volumes
            .iter()
            .find(|s| s.lv_page == ROOT_PAGE_NUMBER)
            .ok_or_else(|| FilesystemError::Parse("Pilot: no root subvolume".into()))?;
        let header_lp = (1..sv.n_pages)
            .find(|&lp| {
                let label = Label::parse(&disk.sectors[sv.pv_page as usize + lp as usize].label);
                label.attributes == attr::HEADER
                    && label.file_id[0] == (client_fid & 0xffff) as u16
                    && label.file_id[1] == (client_fid >> 16) as u16
                    && file_page_number(&label) == 0
            })
            .ok_or_else(|| {
                FilesystemError::Parse("Pilot: client directory has no run-table leader".into())
            })?;
        set_cedar_btree_label_flags(&mut disk, make_file_id(generation, client_fid, header_lp));
    }

    let vol = read_volume(&disk, generation)?;
    let sv = vol
        .physical_root
        .sub_volumes
        .iter()
        .find(|s| s.lv_page == ROOT_PAGE_NUMBER)
        .cloned()
        .ok_or_else(|| FilesystemError::Parse("Pilot: no root subvolume".into()))?;
    let lv_root_vda = sv.pv_page as usize;
    let header_lp = (1..sv.n_pages)
        .find(|&lp| {
            let label = Label::parse(&disk.sectors[lv_root_vda + lp as usize].label);
            label.attributes == attr::HEADER
                && label.file_id[0] == (client_fid & 0xffff) as u16
                && label.file_id[1] == (client_fid >> 16) as u16
                && file_page_number(&label) == 0
        })
        .ok_or_else(|| {
            FilesystemError::Parse("Pilot: client directory has no run-table leader".into())
        })?;
    let d = &mut disk.sectors[lv_root_vda].data;
    wrlong(d, ROOTFILE_CLIENT_WORD, client_fid); // rootFile[client].fp.id
    wrlong(d, ROOTFILE_CLIENT_WORD + 2, header_lp); // fp.da
    wrlong(d, ROOTFILE_CLIENT_WORD + 4, header_lp); // RootFile.page
    set_page_checksum(d);
    Ok(disk)
}

/// If the volume has a Cedar `client` name directory, read it and replace the
/// synthetic / leader names of the files it references with their real names,
/// then drop the directory file itself from the user-visible list.
fn apply_client_names(disk: &Disk, volume: &PilotVolume, files: &mut Vec<PilotFile>) {
    let Some(client_fid) = volume.logical_root.client_fid else {
        return;
    };
    let target = [(client_fid & 0xffff) as u16, (client_fid >> 16) as u16];
    let Some(dir_idx) = files
        .iter()
        .position(|f| f.file_id[0] == target[0] && f.file_id[1] == target[1])
    else {
        return; // directory FileID not found among scanned files
    };
    // The directory file's data pages are its B-tree (header run-table excluded
    // from the scan), already ordered by file page; concatenating yields it.
    let mut client_data = Vec::new();
    for &(_lp, vda) in &files[dir_idx].pages {
        if let Some(s) = disk.sector(vda) {
            client_data.extend_from_slice(&s.data);
        }
    }
    let entries = parse_client_directory(&client_data);
    if entries.is_empty() {
        return;
    }
    // FileID -> name (highest version wins on duplicates).
    let mut by_fid: HashMap<u32, (u16, String)> = HashMap::new();
    for e in entries {
        let slot = by_fid.entry(e.fid).or_insert((e.version, e.name.clone()));
        if e.version >= slot.0 {
            *slot = (e.version, e.name);
        }
    }
    for f in files.iter_mut() {
        let fid = (f.file_id[0] as u32) | ((f.file_id[1] as u32) << 16);
        if let Some((_v, name)) = by_fid.get(&fid) {
            f.name = name.clone();
        }
    }
    files.remove(dir_idx); // the directory file is metadata, not user-visible
    files.sort_by(|a, b| a.name.cmp(&b.name));
    dedup_names(files);
}

// ---- bootingInfo: installing a disk-resident germ / boot file ----
//
// A disk-germ Cedar boot reads its software off the physical volume itself (per
// `DoradoBooting.tioga` §1.3: Cedar microcode boot-loads the installed germ,
// which in turn loads the installed physical-volume boot file). The microcode
// finds those files through the physical-volume root's `bootingInfo` array, and
// reads each one by following a per-run **boot chain** threaded through the
// sector labels' `dontCare` field — the boot path cannot use the file system
// yet, so the chain, not the run table, locates the pages.
//
// This is the install side. The germ + boot-file *content* is a real Mesa VM
// memory image (from `MakeBoot`; unclonable without a source — we have the real
// `Dorado.germ` / `BasicCedarDorado.boot`), but its *placement* is free: the
// image embeds no disk address (confirmed against `BootChannelDisk.mesa` /
// `DiskBootSoft.mc` — the germ reads the boot file's location from `bootingInfo`
// at run time, unlike the Alto `Sys.Boot` snapshot, which is verbatim-only). So
// an installer may lay each file at any free run and record where it went.
//
// Sources: `VolumeFormat.mesa` (`PhysicalRoot.bootingInfo(10B)`), `BootFile.mesa`
// (`DiskFileID`), `File.mesa` (`VolumeFile` ordinals), `DiskBootTransfer.mc`
// (the per-run `bootChainLink`, end-of-file `[-1,-1]`), `BootChannelDisk.mesa`.

/// Word offset of `bootingInfo` in the physical-volume root (`10B`), an
/// `ARRAY File.VolumeFile[checkpoint..bootFile] OF BootFile.DiskFileID`. This is
/// the fixed PV-root offset the disk-boot microcode reads each boot stage from.
pub const BOOTING_INFO_BASE: usize = 8;
/// Word offset of `bootingInfo` in the LOGICAL volume root (`37B`), an
/// `ARRAY File.VolumeFile[checkpoint..debuggee] OF BootFile.DiskFileID` — what
/// Pilot's soft boot (BootTool volume buttons, Booting.Boot) reads.
pub const LV_BOOTING_INFO_BASE: usize = 31;
/// Word offset of `rootFile` in the logical volume root (`125B`), an
/// `ARRAY File.VolumeFile[0..16) OF RECORD[fp: File.FP, page: File.PageNumber]`.
pub const LV_ROOT_FILE_BASE: usize = 85;
/// Words per `LogicalRoot.rootFile` entry: `fp`(4: id 2 + da 2) + `page`(2).
pub const ROOT_FILE_WORDS: usize = 6;
/// Words per `BootFile.DiskFileID`: `fID`(5) + `firstPage`(INT, 2) +
/// `firstLink`(`DiskFace.DontCare`/`DiskAddress`, 2).
pub const DISK_FILE_ID_WORDS: usize = 9;
/// `bootChainLink` end-of-file sentinel (`[-1, -1]`, `DiskBootTransfer.mc`).
const BOOT_CHAIN_EOF: [u16; 2] = [0xffff, 0xffff];

/// A boot-file slot in the PV-root `bootingInfo` array (`File.VolumeFile`
/// ordinals; the disk-boot microcode reads only the PV root's copy). A disk-germ
/// Cedar boot follows [`Germ`](PvBootFile::Germ) then
/// [`BootFile`](PvBootFile::BootFile); [`Microcode`](PvBootFile::Microcode) is
/// the optional soft-microcode slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PvBootFile {
    Checkpoint,
    Microcode,
    Germ,
    BootFile,
}

impl PvBootFile {
    fn ordinal(self) -> usize {
        match self {
            PvBootFile::Checkpoint => 0,
            PvBootFile::Microcode => 1,
            PvBootFile::Germ => 2,
            PvBootFile::BootFile => 3,
        }
    }
    /// Word offset of this slot's `DiskFileID` in the PV root.
    fn root_word(self) -> usize {
        BOOTING_INFO_BASE + self.ordinal() * DISK_FILE_ID_WORDS
    }
    /// Parse a slot name (`germ` / `bootfile` / `microcode` / `checkpoint`).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "germ" => Some(PvBootFile::Germ),
            "bootfile" | "boot" | "pilot" => Some(PvBootFile::BootFile),
            "microcode" | "mc" | "ucode" => Some(PvBootFile::Microcode),
            "checkpoint" => Some(PvBootFile::Checkpoint),
            _ => None,
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            PvBootFile::Checkpoint => "checkpoint",
            PvBootFile::Microcode => "microcode",
            PvBootFile::Germ => "germ",
            PvBootFile::BootFile => "bootFile",
        }
    }
}

/* PilotDiskDefs.mc specifies drive 0 as an Alto-compatible disk with 28
 * sectors and 4,075 virtual cylinders. Its DiskAddress is [cylinder,
 * head,,sector], with the virtual head ignored. A PDI page number is not a
 * DiskAddress: storing a flat VDA aliases distinct cylinder/sector pairs at
 * a boot-chain run boundary. */
const BOOT_DISK_SECTORS: u32 = 28;

fn boot_da_words(vda: u32) -> [u16; 2] {
    [
        (vda / BOOT_DISK_SECTORS) as u16,
        (vda % BOOT_DISK_SECTORS) as u16,
    ]
}
fn boot_vda_from_da(w: [u16; 2]) -> u32 {
    let sector = (w[1] & 0x00ff) as u32;
    (w[0] as u32) * BOOT_DISK_SECTORS + sector
}

/// A parsed PV-root `bootingInfo` slot (`BootFile.DiskFileID`).
#[derive(Debug, Clone)]
pub struct BootFileEntry {
    pub slot: PvBootFile,
    pub file_id: [u16; 5],
    pub first_page: u32,
    /// VDA of the boot file's first page (the initial boot-chain link).
    pub first_link: u32,
}

impl BootFileEntry {
    /// True if the slot is populated (a non-null `fID`).
    pub fn is_present(&self) -> bool {
        self.file_id != [0; 5]
    }
}

/// Read a PV-root `bootingInfo` slot's `DiskFileID`.
pub fn boot_file_entry(disk: &Disk, slot: PvBootFile) -> Result<BootFileEntry, FilesystemError> {
    let d = disk
        .sector(0)
        .ok_or_else(|| FilesystemError::Parse("Pilot: no physical-volume root page".into()))?
        .data
        .as_slice();
    if rdw(d, 0) != PR_SEAL {
        return Err(FilesystemError::Parse(
            "Pilot: bad physical-root seal".into(),
        ));
    }
    let w = slot.root_word();
    let mut file_id = [0u16; 5];
    for (k, fw) in file_id.iter_mut().enumerate() {
        *fw = rdw(d, w + k);
    }
    Ok(BootFileEntry {
        slot,
        file_id,
        first_page: rdlong(d, w + 5),
        first_link: boot_vda_from_da([rdw(d, w + 7), rdw(d, w + 8)]),
    })
}

/// Install `bytes` as the disk-resident boot file for `slot`: allocate free
/// logical pages, write them as a **label-chained Pilot boot file** (`fileID` +
/// ascending `filePage`, `attributes = data`, the per-run `bootChainLink`
/// threaded through `dontCare` and `[-1,-1]` at end of file), record the
/// `DiskFileID` in the PV-root `bootingInfo` slot, and mark the VAM. Returns the
/// updated disk.
///
/// The germ and the physical-volume boot file are the two a disk-germ Cedar boot
/// follows. Content is stored verbatim (page-granular, zero-padded to a page);
/// the boot path locates pages by the chain, so no run-table header is emitted
/// (the boot file's own page 0 is its `BootFile.Header`, i.e. `firstPage = 0`).
pub fn install_boot_file(
    disk: &Disk,
    generation: Generation,
    slot: PvBootFile,
    bytes: &[u8],
) -> Result<Disk, FilesystemError> {
    let vol = read_volume(disk, generation)?;
    let sv = vol
        .physical_root
        .sub_volumes
        .iter()
        .find(|s| s.lv_page == ROOT_PAGE_NUMBER)
        .cloned()
        .ok_or_else(|| FilesystemError::Parse("Pilot: no root subvolume".into()))?;
    let pv_page = sv.pv_page as usize;
    let n_pages = sv.n_pages as usize;
    let n = bytes.len().div_ceil(PAGE_BYTES).max(1);

    /* Keep boot files append-only so a later boot file cannot alter an earlier
     * boot chain's terminating sector. */
    let high_water = (1..n_pages)
        .rev()
        .find(|&lp| {
            disk.sector(pv_page + lp)
                .map(|s| !is_free_label(&Label::parse(&s.label), vol.logical_root.v_id, generation))
                .unwrap_or(true)
        })
        .unwrap_or(0);
    let free: Vec<u32> = ((high_water + 1)..n_pages)
        .filter(|&lp| {
            disk.sector(pv_page + lp)
                .map(|s| is_free_label(&Label::parse(&s.label), vol.logical_root.v_id, generation))
                .unwrap_or(false)
        })
        .map(|lp| lp as u32)
        .collect();
    let slots_needed = n;
    if free.len() < slots_needed {
        return Err(FilesystemError::DiskFull(format!(
            "Pilot: need {slots_needed} free pages to install the {} boot file ({} bytes), have {}",
            slot.label(),
            bytes.len(),
            free.len()
        )));
    }
    let pages = free[..n].to_vec();
    let vdas: Vec<u32> = pages.iter().map(|&lp| pv_page as u32 + lp).collect();

    let mut disk = disk.clone();

    // Allocate a FileID for the boot file (advance the LV-root counter).
    let lv_root_vda = pv_page;
    let fid = rdlong(&disk.sectors[lv_root_vda].data, 253).wrapping_add(1);
    let file_id = make_file_id(generation, fid, 0);

    /* PilotDisk.mc clears the low file-flag bits after every successful
     * sector.  BootChannelDisk's two reusable operations start their first
     * data transfer at file page 0 and the second at page 2; subsequent
     * labels retain their RelID/filePage but carry `freePage` flags. */

    // Lay the pages and thread the boot chain through the labels' `dontCare`.
    for (i, (&lp, &vda)) in pages.iter().zip(&vdas).enumerate() {
        let ends_run = i + 1 == n;
        let dont_care = if i + 1 == n {
            BOOT_CHAIN_EOF // last page of the file
        } else if ends_run || vdas[i + 1] != vda + 1 {
            boot_da_words(vdas[i + 1]) // next physical run's first page
        } else {
            [0, 0] // interior of a contiguous run (link unused)
        };
        let s = &mut disk.sectors[pv_page + lp as usize];
        let mut label = Label::new(
            file_id,
            i as u32,
            if i == 0 || i == 2 {
                attr::DATA
            } else {
                attr::FREE_PAGE
            },
        );
        label.dont_care = dont_care;
        s.label = label.bytes();
        let off = i * PAGE_BYTES;
        let end = (off + PAGE_BYTES).min(bytes.len());
        let chunk = &bytes[off..end];
        for b in s.data.iter_mut() {
            *b = 0;
        }
        s.data[..chunk.len()].copy_from_slice(chunk);
    }

    // Record the DiskFileID in the PV-root bootingInfo slot.
    let first = boot_da_words(vdas[0]);
    {
        let d = &mut disk.sectors[0].data;
        let w = slot.root_word();
        for (k, &v) in file_id.iter().enumerate() {
            wrw(d, w + k, v);
        }
        wrlong(d, w + 5, 0); // firstPage
        wrw(d, w + 7, first[0]);
        wrw(d, w + 8, first[1]);
        set_page_checksum(d);
    }

    /* Record the same file in the LOGICAL volume root. Pilot's soft boot
     * (BootTool's herald volume buttons, Booting.Boot, RollBack) resolves the
     * target volume's boot files through `LogicalRoot.bootingInfo` (37B) and
     * `LogicalRoot.rootFile` (125B) -- File.SetRoot/RecordRootFile write both
     * on a real Othello install (FileImpl.mesa). Without these a boot button
     * raises an uncaught File.Error from FileImpl. Same VolumeFile ordinals
     * as the PV array; rootFile is RECORD[fp: File.FP{id(2), da(2)}, page(2)]
     * with fp mirroring the label fileID words and page 0 (boot chain start). */
    {
        let d = &mut disk.sectors[lv_root_vda].data;
        let bw = LV_BOOTING_INFO_BASE + slot.ordinal() * DISK_FILE_ID_WORDS;
        for (k, &v) in file_id.iter().enumerate() {
            wrw(d, bw + k, v);
        }
        wrlong(d, bw + 5, 0); // firstPage
        wrw(d, bw + 7, first[0]);
        wrw(d, bw + 8, first[1]);
        let rw = LV_ROOT_FILE_BASE + slot.ordinal() * ROOT_FILE_WORDS;
        for (k, &v) in file_id.iter().take(4).enumerate() {
            wrw(d, rw + k, v); // fp.id + fp.da = label fileID words
        }
        wrlong(d, rw + 4, 0); // page
    }

    // Persist the advanced FileID, re-checksum the LV root, refresh the VAM.
    wrlong(&mut disk.sectors[lv_root_vda].data, 253, fid);
    set_page_checksum(&mut disk.sectors[lv_root_vda].data);
    rebuild_vam(&mut disk, &sv);
    Ok(disk)
}

/// Follow a boot file's per-run `bootChainLink` chain from its `bootingInfo`
/// slot, returning the concatenated page bytes (page-granular), or `None` if the
/// slot is empty. Validates each page's label `fileID` + `filePage` and stops at
/// the `[-1, -1]` end-of-file sentinel — the same walk the boot microcode does,
/// so a successful read is the structural proof that the install is well-formed.
pub fn read_boot_file(disk: &Disk, slot: PvBootFile) -> Result<Option<Vec<u8>>, FilesystemError> {
    let entry = boot_file_entry(disk, slot)?;
    if !entry.is_present() {
        return Ok(None);
    }
    let mut out = Vec::new();
    let mut vda = entry.first_link;
    let limit = disk.geometry.total_sectors() as u32 + 1;
    for i in 0..limit {
        let expected_page = entry.first_page + i;
        let s = disk.sector(vda as usize).ok_or_else(|| {
            FilesystemError::Parse(format!("Pilot: boot chain page {vda} out of range"))
        })?;
        let l = Label::parse(&s.label);
        if l.file_id != entry.file_id {
            return Err(FilesystemError::Parse(format!(
                "Pilot: boot chain page {vda} fileID mismatch: have {:?}, want {:?}",
                l.file_id, entry.file_id
            )));
        }
        if l.file_page != expected_page {
            return Err(FilesystemError::Parse(format!(
                "Pilot: boot chain page {vda} filePage {} != expected {expected_page}",
                l.file_page
            )));
        }
        out.extend_from_slice(&s.data);
        if l.dont_care == BOOT_CHAIN_EOF {
            return Ok(Some(out));
        }
        vda = if l.dont_care == [0, 0] {
            vda + 1
        } else {
            boot_vda_from_da(l.dont_care)
        };
    }
    Err(FilesystemError::Parse(
        "Pilot: boot chain exceeds disk size (missing [-1,-1] terminator?)".into(),
    ))
}

/// A Pilot-shaped geometry of `total_pages` 512-byte pages (single spindle).
/// Geometry is bookkeeping for PDI; any factorization works for tooling.
pub fn pilot_geometry(total_pages: u16) -> Geometry {
    Geometry {
        family: FsFamily::Pilot,
        disk_model: 0,
        n_disks: 1,
        n_cylinders: total_pages,
        n_heads: 1,
        n_sectors: 1,
        label_bytes: LABEL_BYTES as u16,
        data_bytes: PAGE_BYTES as u16,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_matches_mesa_properties() {
        // All-zero page -> checksum 0; idempotent normalization of 0xFFFF.
        assert_eq!(pilot_checksum(&[0u16; 255]), 0);
        // A known small case: single word rotate behavior is stable.
        let a = pilot_checksum(&[1, 2, 3]);
        let b = pilot_checksum(&[1, 2, 3]);
        assert_eq!(a, b);
    }

    #[test]
    fn label_round_trips() {
        let l = Label {
            file_id: [0x1111, 0x2222, 0x3333, 0x4444, 0x5555],
            file_page: 0x0007_0001,
            attributes: attr::DATA,
            dont_care: [0xabcd, 0xef01],
        };
        let back = Label::parse(&l.bytes());
        assert_eq!(l, back);
        // filePage is low-word-first: word5 = 0x0001, word6 = 0x0007.
        let b = l.bytes();
        assert_eq!(rdw(&b, 5), 0x0001);
        assert_eq!(rdw(&b, 6), 0x0007);
    }

    #[test]
    fn blank_volume_round_trips() {
        let geo = pilot_geometry(256);
        let disk = create_blank(geo, Generation::CedarNucleus, "TestVol").expect("create");

        let vol = read_volume(&disk, Generation::CedarNucleus).expect("read");
        assert_eq!(vol.pv_label, "TestVol");
        assert_eq!(vol.lv_label, "TestVol");
        assert_eq!(vol.volume_type, 3); // cedar
        assert_eq!(vol.physical_root.sub_volumes.len(), 1);

        // volume_size = total - OTHELLO_PV_RESERVE - 1 (marker). The LV root
        // and the VAM file (2 headers + bitmap data pages) are in use; the rest
        // are free, and the VAM bitmap must agree with the label scan.
        let expected_size = 256 - OTHELLO_PV_RESERVE as u32 - 1;
        assert_eq!(vol.volume_size, expected_size);
        let vam_pages = 2 + vam_data_pages(expected_size) as u32;
        assert_eq!(vol.free_pages, expected_size - 1 - vam_pages);
        assert_eq!(vol.vam_free_pages, Some(vol.free_pages));
    }

    #[test]
    fn blank_volume_pdi_round_trips() {
        use super::super::pdi;
        let geo = pilot_geometry(128);
        let disk = create_blank(geo, Generation::CedarNucleus, "PdiVol").expect("create");
        let bytes = pdi::write(&disk);
        let back = pdi::read(&bytes).expect("pdi read");
        assert_eq!(back.geometry.family, FsFamily::Pilot);
        let vol = read_volume(&back, Generation::CedarNucleus).expect("read");
        assert_eq!(vol.lv_label, "PdiVol");
    }

    #[test]
    fn add_file_enumerates_and_reads_back() {
        use crate::fs::filesystem::Filesystem;

        let geo = pilot_geometry(128);
        let blank = create_blank(geo, Generation::CedarNucleus, "Files").expect("create");
        let free_before = read_volume(&blank, Generation::CedarNucleus)
            .unwrap()
            .free_pages;

        // A 700-byte file -> 2 data pages + 2 headers = 4 pages consumed.
        let payload: Vec<u8> = (0..700u32).map(|i| (i % 251) as u8).collect();
        let (disk, fid) = add_file(&blank, Generation::CedarNucleus, &payload).expect("add_file");
        assert_eq!(fid, 2); // FileID 1 is reserved for the VAM

        let vol = read_volume(&disk, Generation::CedarNucleus).unwrap();
        assert_eq!(vol.free_pages, free_before - 4);
        // The VAM bitmap was rebuilt and agrees with the labels.
        assert_eq!(vol.vam_free_pages, Some(vol.free_pages));

        // DiskFace.mesa defines a Cedar relID as the full File.FP: the
        // two-word FileID followed by the header's two-word DA.  Cedar's
        // verified disk reads reject a label that carries only the FileID.
        let sv = &vol.physical_root.sub_volumes[0];
        let header = (1..sv.n_pages as usize)
            .map(|lp| {
                (
                    lp,
                    Label::parse(&disk.sectors[sv.pv_page as usize + lp].label),
                )
            })
            .find(|(_, l)| l.attributes == attr::HEADER && l.file_id[..2] == [fid as u16, 0])
            .expect("new file header");
        assert_eq!(header.1.file_id[2], header.0 as u16);
        assert_eq!(header.1.file_id[3], 0);
        let free = (1..sv.n_pages as usize)
            .map(|lp| Label::parse(&disk.sectors[sv.pv_page as usize + lp].label))
            .find(|l| is_free_label(l, vol.logical_root.v_id, Generation::CedarNucleus))
            .expect("free page");
        assert_eq!(free.file_id, vol.logical_root.v_id);

        let mut fs = PilotFilesystem::open(disk, Generation::CedarNucleus).expect("open");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1, "one file present");
        assert_eq!(entries[0].size, 2 * PAGE_BYTES as u64); // page-granular

        let got = fs.read_file(&entries[0], usize::MAX).unwrap();
        assert_eq!(got.len(), 2 * PAGE_BYTES);
        assert_eq!(&got[..payload.len()], &payload[..]); // data preserved
        assert!(got[payload.len()..].iter().all(|&b| b == 0)); // zero-padded
    }

    #[test]
    fn delete_frees_pages_and_multi_run_reallocates() {
        use crate::fs::filesystem::Filesystem;

        let geo = pilot_geometry(128);
        let blank = create_blank(geo, Generation::CedarNucleus, "Frag").expect("create");
        let free0 = read_volume(&blank, Generation::CedarNucleus)
            .unwrap()
            .free_pages;

        // Two single-page files: A (fid 1) then B (fid 2), contiguous.
        let a: Vec<u8> = vec![0xAA; 100];
        let b: Vec<u8> = vec![0xBB; 100];
        let (d1, fa) = add_file(&blank, Generation::CedarNucleus, &a).unwrap();
        let (d2, _fb) = add_file(&d1, Generation::CedarNucleus, &b).unwrap();
        assert_eq!(fa, 2); // FileID 1 is reserved for the VAM
                           // Each file = 2 headers + 1 data = 3 pages; 6 consumed.
        assert_eq!(
            read_volume(&d2, Generation::CedarNucleus)
                .unwrap()
                .free_pages,
            free0 - 6
        );

        // Delete A: frees its 3 pages, leaving a gap before B's pages.
        let d3 = delete_file(&d2, fa).unwrap();
        assert_eq!(
            read_volume(&d3, Generation::CedarNucleus)
                .unwrap()
                .free_pages,
            free0 - 3
        );

        // A 3-data-page file must now reuse the freed gap + later free pages,
        // i.e. a non-contiguous (multi-run) allocation. Read it back intact.
        let c: Vec<u8> = (0..(3 * PAGE_BYTES) as u32 - 10)
            .map(|i| (i % 253) as u8)
            .collect();
        let (d4, fc) = add_file(&d3, Generation::CedarNucleus, &c).unwrap();
        let mut fs = PilotFilesystem::open(d4, Generation::CedarNucleus).unwrap();
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        // B and C remain.
        assert_eq!(files.len(), 2);
        let c_entry = files
            .iter()
            .find(|f| f.name.contains(&format!("{fc:04X}")))
            .expect("file C present");
        let got = fs.read_file(c_entry, c.len()).unwrap();
        assert_eq!(&got[..], &c[..]);
    }

    #[test]
    fn original_pilot_generation_persists_and_uses_80bit_ids() {
        use super::super::pdi;

        // Create an original-Pilot volume, write it as PDI, and confirm the
        // generation survives via flags bit 2.
        let geo = pilot_geometry(128);
        let disk = create_blank(geo, Generation::OriginalPilot, "PilotGen").expect("create");
        let bytes = write_pdi(&disk, Generation::OriginalPilot);
        let header = pdi::read_header(&bytes).expect("header");
        assert!(header.flags & pdi::FLAG_PILOT_80BIT != 0, "flags bit 2 set");

        // Reopen, add a file as original Pilot, and check the label fileID is a
        // real 80-bit UID (word 4 carries the creator prefix, != a Cedar FP).
        let reopened = pdi::read(&bytes).expect("read");
        let (disk, fid) = add_file(&reopened, Generation::OriginalPilot, b"hi").expect("add");
        assert_eq!(fid, 2);
        let vol = read_volume(&disk, Generation::OriginalPilot).unwrap();
        // The added file's header page carries the 80-bit UID.
        let sv = &vol.physical_root.sub_volumes[0];
        let mut found = None;
        for lp in 1..sv.n_pages as usize {
            let l = Label::parse(&disk.sector(sv.pv_page as usize + lp).unwrap().label);
            if l.attributes == attr::HEADER && l.file_id[..2] != VAM_FILE_ID[..2] {
                found = Some(l.file_id);
            }
        }
        let id = found.expect("user file header");
        assert_eq!(id[0], 2);
        assert_eq!([id[2], id[3], id[4]], PILOT_UID_CREATOR); // 80-bit, not FP fill
    }

    #[test]
    fn seal_is_the_root_validity_gate() {
        let geo = pilot_geometry(128);
        let mut disk = create_blank(geo, Generation::CedarNucleus, "X").expect("create");
        assert!(read_volume(&disk, Generation::CedarNucleus).is_ok());
        // Real Pilot never computes the root-page checksum word (it stays 0), so
        // corrupting a non-seal data word must NOT fail the read...
        disk.sectors[0].data[10] ^= 0xff;
        assert!(read_volume(&disk, Generation::CedarNucleus).is_ok());
        // ...but a bad physical-root seal (word 0) must.
        disk.sectors[0].data[0] ^= 0xff;
        assert!(read_volume(&disk, Generation::CedarNucleus).is_err());
    }

    #[test]
    fn install_boot_file_round_trips_via_chain() {
        use super::super::pdi;

        let geo = pilot_geometry(256);
        let blank = create_blank(geo, Generation::CedarNucleus, "Boot").expect("create");

        // A ~6-page "germ" with non-trivial content.
        let germ: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
        let disk = install_boot_file(&blank, Generation::CedarNucleus, PvBootFile::Germ, &germ)
            .expect("install germ");

        // bootingInfo[germ] is populated; firstPage is 0; firstLink lands inside
        // the subvolume's logical pages.
        let entry = boot_file_entry(&disk, PvBootFile::Germ).unwrap();
        assert!(entry.is_present());
        assert_eq!(entry.first_page, 0);
        let sv = &read_volume(&disk, Generation::CedarNucleus)
            .unwrap()
            .physical_root
            .sub_volumes[0];
        assert!(entry.first_link >= sv.pv_page && entry.first_link < sv.pv_page + sv.n_pages);

        // Following the chain (the boot microcode's own walk) recovers the bytes,
        // zero-padded to a page boundary.
        let got = read_boot_file(&disk, PvBootFile::Germ)
            .unwrap()
            .expect("germ present");
        let padded = germ.len().div_ceil(PAGE_BYTES) * PAGE_BYTES;
        assert_eq!(got.len(), padded);
        assert_eq!(&got[..germ.len()], &germ[..]);
        assert!(got[germ.len()..].iter().all(|&b| b == 0));

        // An un-installed slot reads back as empty.
        assert!(read_boot_file(&disk, PvBootFile::BootFile)
            .unwrap()
            .is_none());

        // Installing a second slot leaves the first intact, and both survive a
        // PDI round trip byte-for-byte through the chain.
        let boot: Vec<u8> = (0..5000u32)
            .map(|i| (i.wrapping_mul(7) % 256) as u8)
            .collect();
        let disk = install_boot_file(&disk, Generation::CedarNucleus, PvBootFile::BootFile, &boot)
            .expect("install bootFile");
        let bytes = write_pdi(&disk, Generation::CedarNucleus);
        let back = pdi::read(&bytes).expect("pdi read");
        assert_eq!(
            read_boot_file(&back, PvBootFile::Germ).unwrap().unwrap(),
            got
        );
        let boot_back = read_boot_file(&back, PvBootFile::BootFile)
            .unwrap()
            .expect("bootFile present");
        assert_eq!(&boot_back[..boot.len()], &boot[..]);
        // The volume still parses and the VAM agrees with the labels.
        let vol = read_volume(&back, Generation::CedarNucleus).unwrap();
        assert_eq!(vol.vam_free_pages, Some(vol.free_pages));

        // The LOGICAL volume root mirrors each installed slot: bootingInfo (37B)
        // carries the same DiskFileID words as the PV slot, and rootFile (125B)
        // carries fp = the label fileID words + page 0 -- what Pilot's soft boot
        // (BootTool volume buttons) resolves. The root page checksum still
        // verifies after the in-place update.
        let sv = &read_volume(&back, Generation::CedarNucleus)
            .unwrap()
            .physical_root
            .sub_volumes[0];
        let lv = back.sector(sv.pv_page as usize).unwrap().data.as_slice();
        for slot in [PvBootFile::Germ, PvBootFile::BootFile] {
            let pv = back.sector(0).unwrap().data.as_slice();
            let pw = slot.root_word();
            let bw = LV_BOOTING_INFO_BASE + slot.ordinal() * DISK_FILE_ID_WORDS;
            for k in 0..DISK_FILE_ID_WORDS {
                assert_eq!(rdw(lv, bw + k), rdw(pv, pw + k), "LV bootingInfo word {k}");
            }
            let rw = LV_ROOT_FILE_BASE + slot.ordinal() * ROOT_FILE_WORDS;
            for k in 0..4 {
                assert_eq!(rdw(lv, rw + k), rdw(pv, pw + k), "LV rootFile fp word {k}");
            }
            assert_eq!(rdw(lv, rw + 4), 0);
            assert_eq!(rdw(lv, rw + 5), 0);
        }
        let words: Vec<u16> = (0..PAGE_WORDS - 1).map(|i| rdw(lv, i)).collect();
        assert_eq!(rdw(lv, PAGE_WORDS - 1), pilot_checksum(&words));
    }

    #[test]
    fn boot_chain_append_avoids_old_run_breaks() {
        // A boot installer appends after the high-water mark rather than
        // consuming an earlier file's chain-break gaps.
        let geo = pilot_geometry(160);
        let blank = create_blank(geo, Generation::CedarNucleus, "Frag").expect("create");

        // Three single-page files (each = header + data = 2 pages), then delete
        // the middle one to leave a hole between B's pages and the later free run.
        let (d1, _fa) = add_file(&blank, Generation::CedarNucleus, &[0xAA; 50]).unwrap();
        let (d2, fb) = add_file(&d1, Generation::CedarNucleus, &[0xBB; 50]).unwrap();
        let (d3, _fc) = add_file(&d2, Generation::CedarNucleus, &[0xCC; 50]).unwrap();
        let d4 = delete_file(&d3, fb).unwrap();

        // A 4-page boot file must now reuse the freed hole plus later free pages,
        // i.e. a multi-run allocation with a real chain link.
        let payload: Vec<u8> = (0..(4 * PAGE_BYTES) as u32 - 7)
            .map(|i| (i % 249) as u8)
            .collect();
        let disk = install_boot_file(&d4, Generation::CedarNucleus, PvBootFile::Germ, &payload)
            .expect("install");

        // The chain follower reconstructs the bytes across the run break.
        let got = read_boot_file(&disk, PvBootFile::Germ)
            .unwrap()
            .expect("present");
        assert_eq!(&got[..payload.len()], &payload[..]);

        // Confirm append allocation did not repurpose the earlier gaps as a
        // new boot-chain run.
        let entry = boot_file_entry(&disk, PvBootFile::Germ).unwrap();
        let mut vda = entry.first_link;
        let mut saw_link = false;
        loop {
            let l = Label::parse(&disk.sector(vda as usize).unwrap().label);
            if l.dont_care == BOOT_CHAIN_EOF {
                break;
            }
            if l.dont_care != [0, 0] {
                saw_link = true;
                vda = boot_vda_from_da(l.dont_care);
            } else {
                vda += 1;
            }
        }
        assert!(!saw_link, "append allocation should remain contiguous");
    }

    #[test]
    fn client_directory_btree_build_parse_round_trips() {
        // The B-tree the writer emits is parsed back to the same name->FileID set
        // (the format-level contract, independent of the nucleus file plumbing).
        let entries = vec![
            ClientDirEntry {
                name: "Compiler.bcd".into(),
                version: 3,
                fid: 42,
                da: 11,
            },
            ClientDirEntry {
                name: "Tioga.bcd".into(),
                version: 1,
                fid: 7,
                da: 12,
            },
            ClientDirEntry {
                name: "a".into(),
                version: 1,
                fid: 99,
                da: 13,
            }, // odd length
        ];
        let bytes = build_client_directory(&entries).expect("build");
        assert_eq!(
            bytes.len(),
            PAGE_BYTES * BTREE_INITIAL_FILE_PAGES,
            "FS's fixed initial directory/cache backing file"
        );
        assert_eq!(rdw(&bytes, 0), BTREE_SEAL);
        assert_eq!(rdlong(&bytes, 5), 3, "entryCount in statePage");

        let got = parse_client_directory(&bytes);
        let mut map: std::collections::HashMap<u32, (u16, String, u32)> = Default::default();
        for e in got {
            map.insert(e.fid, (e.version, e.name, e.da));
        }
        // fp.da travels with the entry: Cedar dereferences it as
        // VolumeFormat.LogicalRun[first: fp.da] to reach the file's header
        // (FileImpl.Open). It used to be written as 0, which made Cedar reject
        // the enumeration with FS.Error $badFP.
        assert_eq!(map[&42], (3, "Compiler.bcd".into(), 11));
        assert_eq!(map[&7], (1, "Tioga.bcd".into(), 12));
        assert_eq!(map[&99], (1, "a".into(), 13));
    }

    #[test]
    fn client_directory_spans_multiple_pages() {
        // Far more names than one ~1022-word leaf holds: the writer must build a
        // 2-level B-tree (root + several leaves), and parse_client_directory --
        // which walks every tree page -- must recover every entry exactly once,
        // with its FileID and fp.da intact. Regression guard for the multi-page
        // writer (2026-07-20); before it, this returned DiskFull.
        let entries: Vec<ClientDirEntry> = (0..200)
            .map(|i| ClientDirEntry {
                name: format!("AVeryLongCedarFileName-{i:04}.bcd"),
                version: 1,
                fid: 1000 + i,
                da: 100 + i,
            })
            .collect();
        let bytes = build_client_directory(&entries).expect("multi-page build");
        // depth must be 2 and the root must not be page 1 (a single leaf).
        assert_eq!(rdw(&bytes, 7), 2, "expected a 2-level tree");
        assert!(rdw(&bytes, 2) > 1, "root should be an internal page");
        let got = parse_client_directory(&bytes);
        let mut map: std::collections::HashMap<u32, (String, u32)> = Default::default();
        for e in got {
            map.insert(e.fid, (e.name, e.da));
        }
        assert_eq!(map.len(), 200, "every entry recovered exactly once");
        for i in 0..200u32 {
            let (name, da) = &map[&(1000 + i)];
            assert_eq!(name, &format!("AVeryLongCedarFileName-{i:04}.bcd"));
            assert_eq!(*da, 100 + i, "fp.da preserved across pages");
        }
    }

    #[test]
    fn client_directory_names_files_through_full_stack() {
        use super::super::pdi;
        use crate::fs::filesystem::Filesystem;

        // FSFileOpsImpl reserves a 128-page client directory/cache file.
        let geo = pilot_geometry(512);
        let blank = create_blank(geo, Generation::CedarNucleus, "Named").expect("create");

        // Two user files; FileID 1 is the VAM, so these get 2 and 3.
        let pa: Vec<u8> = (0..600u32).map(|i| (i % 251) as u8).collect();
        let pb: Vec<u8> = (0..50u32)
            .map(|i| (i.wrapping_mul(3) % 256) as u8)
            .collect();
        let (disk, fa) = add_file(&blank, Generation::CedarNucleus, &pa).expect("add a");
        let (disk, fb) = add_file(&disk, Generation::CedarNucleus, &pb).expect("add b");

        // Before installing the directory: synthetic names, no client dir.
        assert!(read_volume(&disk, Generation::CedarNucleus)
            .unwrap()
            .logical_root
            .client_fid
            .is_none());
        {
            let mut fs = PilotFilesystem::open(disk.clone(), Generation::CedarNucleus).unwrap();
            let root = fs.root().unwrap();
            let names: Vec<String> = fs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .map(|e| e.name)
                .collect();
            assert_eq!(names.len(), 2);
            assert!(
                names.iter().all(|n| n.starts_with("LV0_")),
                "synthetic: {names:?}"
            );
        }

        // Install the name directory mapping each FileID to a human name.
        let dir = vec![
            ("Hello.mesa".to_string(), 1u16, fa),
            ("Othello.boot".to_string(), 2u16, fb),
        ];
        let disk = set_client_directory(&disk, Generation::CedarNucleus, &dir).expect("set dir");

        // Every named file must resolve to a real header page -- the value
        // set_client_directory records as File.FP.da. A null da points at the
        // logical-volume root and Cedar rejects the enumeration with
        // FS.Error $badFP. Regression guard for the 2026-07-20 fix.
        {
            let vol = read_volume(&disk, Generation::CedarNucleus).unwrap();
            let sv = vol
                .physical_root
                .sub_volumes
                .iter()
                .find(|s| s.lv_page == ROOT_PAGE_NUMBER)
                .cloned()
                .unwrap();
            let da_a = header_lp_for(&disk, &sv, fa).expect("file a has a header");
            let da_b = header_lp_for(&disk, &sv, fb).expect("file b has a header");
            assert_ne!(da_a, 0, "fp.da must not be the volume root");
            assert_ne!(da_b, 0, "fp.da must not be the volume root");
            assert_ne!(da_a, da_b, "each file has its own header page");
        }

        assert_eq!(
            read_volume(&disk, Generation::CedarNucleus)
                .unwrap()
                .logical_root
                .client_fid,
            Some(4), // VAM=1, files 2 & 3, directory file = 4
        );
        let lv_root = &disk.sectors[84].data;
        let client_da = rdlong(lv_root, ROOTFILE_CLIENT_WORD + 2);
        assert_ne!(client_da, 0, "Cedar client root needs a leader hint");
        assert_eq!(
            rdlong(lv_root, ROOTFILE_CLIENT_WORD + 4),
            client_da,
            "RootFile.page and fp.da identify the same leader"
        );

        // Through a PDI round trip, the browse view shows the real names (the
        // directory file itself is hidden), and the data still reads back.
        let pdi = write_pdi(&disk, Generation::CedarNucleus);
        let back = pdi::read(&pdi).expect("pdi read");
        let mut fs = PilotFilesystem::open(back, Generation::CedarNucleus).expect("open");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["Hello.mesa", "Othello.boot"],
            "real names, dir hidden"
        );

        let hello = entries.iter().find(|e| e.name == "Hello.mesa").unwrap();
        let got = fs.read_file(hello, usize::MAX).unwrap();
        assert_eq!(&got[..pa.len()], &pa[..], "file content preserved");
    }
}
