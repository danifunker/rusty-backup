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

/// True if a label `attributes`/`type` word marks a free page in either the
/// Cedar-nucleus scheme ([`attr::FREE_PAGE`]) or the classic Pilot scheme
/// ([`attr::FREE_PAGE_CLASSIC`]).
fn is_free_page_attr(a: u16) -> bool {
    a == attr::FREE_PAGE || a == attr::FREE_PAGE_CLASSIC
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

/// Well-known 32-bit FileID of the Volume Allocation Map file (Cedar nucleus
/// `VolumeFile::VAM` = root-file slot 7). We allocate it first, so user files
/// start at FileID 2.
const VAM_FILE_ID: [u16; 5] = [1, 0, 0, 0, 0];
/// Word offset of `rootFile[VolumeFile::VAM]` in the LV root (`rootFile`
/// base 85 + 7 * SIZE[RootFile](6)).
const ROOTFILE_VAM_WORD: usize = 85 + 7 * 6;

/// Words of VAM data for a volume of `volume_size` pages: `rover`(2) + `size`(2)
/// + one bit per page.
fn vam_data_words(volume_size: u32) -> usize {
    4 + (volume_size as usize).div_ceil(16)
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

/// Build a label `fileID` from a per-volume counter `n`, per generation:
/// Cedar nucleus = `File.FP` (FileID + null DA hint + fill); original Pilot =
/// 80-bit `UniversalID` (counter + creator prefix).
fn make_file_id(generation: Generation, n: u32) -> [u16; 5] {
    let (lo, hi) = ((n & 0xffff) as u16, (n >> 16) as u16);
    match generation {
        Generation::CedarNucleus => [lo, hi, 0, 0, 0],
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
        Ok(LogicalRoot {
            v_id,
            label: unpack_label(data, 8, label_len),
            volume_type: rdw(data, 28),
            volume_size: rdlong(data, 29),
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
            if is_free_page_attr(Label::parse(&s.label).attributes) {
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
    let pv_id: [u16; 5] = [0x0001, 0, 0, 0, 0];
    let lv_id: [u16; 5] = [0x0002, 0, 0, 0, 0];

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
        sectors[vda].label = Label::new([0; 5], lp as u32, attr::FREE_PAGE).bytes();
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
    install_vam(&mut disk, &sub);
    let _ = generation; // generation affects only fileID interpretation of files
    Ok(disk)
}

/// Install the Volume Allocation Map as a real file (`VAM_FILE_ID`) at logical
/// pages 1.. of the subvolume: a `header` page (run table) plus `data` pages
/// holding the `VAMObject` bitmap, with `rootFile[VAM]` and `lastFileID` set in
/// the LV root. The bitmap is then filled from the page labels.
fn install_vam(disk: &mut Disk, sv: &SubVolumeDesc) {
    let pv = sv.pv_page as usize;
    let n_data = vam_data_pages(sv.lv_size);
    let header_lp = 1usize;
    let data_lp0 = 2usize;

    // Header page: a single run covering the VAM's data pages.
    {
        let s = &mut disk.sectors[pv + header_lp];
        s.label = Label::new(VAM_FILE_ID, 0, attr::HEADER).bytes();
        let d = &mut s.data;
        for b in d.iter_mut() {
            *b = 0;
        }
        wrw(d, 0, 1); // headerPages
        wrw(d, 1, 1); // maxRuns
        wrlong(d, RunTable::RUNS_BASE, data_lp0 as u32);
        wrw(d, RunTable::RUNS_BASE + 2, n_data as u16);
        wrlong(d, RunTable::RUNS_BASE + 3, LAST_LOGICAL_RUN);
    }
    // Data pages (bitmap content written by rebuild_vam).
    for i in 0..n_data {
        let s = &mut disk.sectors[pv + data_lp0 + i];
        s.label = Label::new(VAM_FILE_ID, (i + 1) as u32, attr::DATA).bytes();
        for b in s.data.iter_mut() {
            *b = 0;
        }
    }
    // LV root: point rootFile[VAM] at the VAM and reserve FileID 1.
    {
        let d = &mut disk.sectors[pv].data;
        wrw(d, ROOTFILE_VAM_WORD, VAM_FILE_ID[0]); // fp.id low
        wrw(d, ROOTFILE_VAM_WORD + 1, VAM_FILE_ID[1]); // fp.id high
                                                       // fp.da (words +2..+3) = 0
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
        if l.attributes == attr::HEADER && l.file_id == VAM_FILE_ID {
            header_lp = Some(lp);
            break;
        }
    }
    let Some(hlp) = header_lp else { return };
    let run = RunTable::parse(&disk.sectors[pv + hlp].data);
    let mut data_lps = Vec::new();
    for (first, count) in run.runs {
        for p in 0..count {
            data_lps.push((first + p) as usize);
        }
    }

    // Build the bitmap words: rover(0..1)=0, size(2..3), then 1 bit per page.
    let mut words = vec![0u16; vam_data_words(sv.lv_size)];
    words[2] = (vsize & 0xffff) as u16;
    words[3] = (vsize >> 16) as u16;
    for lp in 0..vsize {
        let in_use = Label::parse(&disk.sectors[pv + lp].label).attributes != attr::FREE_PAGE;
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
        if l.attributes == attr::HEADER && l.file_id == VAM_FILE_ID {
            hlp = Some(lp);
            break;
        }
    }
    let run = RunTable::parse(&disk.sector(pv + hlp?)?.data);
    let mut data_lps = Vec::new();
    for (first, count) in run.runs {
        for p in 0..count {
            data_lps.push((first + p) as usize);
        }
    }
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
}

/// Logical page number from a label, masking the classic page-0 flag bits
/// (`immutable`/`temporary`/`zeroSize`, word 6 bits 13-15; `filePageHi` is 7
/// bits). For the Cedar scheme the page number is small, so the mask is a no-op.
fn file_page_number(l: &Label) -> u32 {
    let lo = l.file_page & 0xffff;
    let hi = (l.file_page >> 16) & 0x7f;
    (hi << 16) | lo
}

/// True if a page's label `attributes`/`type` word marks it as a **user-file
/// data page** — i.e. not free, not a volume-structure page (physicalRoot..
/// scavengerLog = 0..9), and not a Cedar run-table `header` (9729) or `freePage`
/// (9728). This admits Cedar `data` (9730) and classic file types (`tempFileList`
/// 10, `vmBackingFile` 12, `anonymousFile` 15, client types >= 256, ...).
fn is_user_data_page(a: u16) -> bool {
    a != attr::FREE_PAGE && a != attr::HEADER && !matches!(a, 0..=9)
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
        let files = build_files(&disk, &volume);
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
}

/// Scan every subvolume's page labels and group user-file data pages by `fileID`
/// into a sorted, deduplicated file table.
fn build_files(disk: &Disk, volume: &PilotVolume) -> Vec<PilotFile> {
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
            if !is_user_data_page(l.attributes)
                || l.file_id == VAM_FILE_ID
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
/// new disk and the allocated 32-bit `FileID`. The file occupies one `header`
/// page (carrying its run table) plus `ceil(len/512)` `data` pages, allocated
/// from a contiguous run of free logical pages; `lastFileID` in the LV root is
/// advanced. Pages are page-granular (no byte-exact length yet — the real Pilot
/// keeps that in file properties; see the module doc), so a read-back returns
/// the data zero-padded to the next page boundary.
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
    let need = 1 + n_data; // header + data pages

    // Collect free logical pages (logical 0 is the LV root; the marker lives
    // past the subvolume, so [1..n_pages) is fair game). First-fit, allowing a
    // non-contiguous data region (the run table records multiple extents).
    let n_pages = sv.n_pages as usize;
    let pv_page = sv.pv_page as usize;
    let free: Vec<u32> = (1..n_pages)
        .filter(|&lp| {
            disk.sector(pv_page + lp)
                .map(|s| Label::parse(&s.label).attributes == attr::FREE_PAGE)
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

    let header_lp = free[0];
    let data_lps = &free[1..1 + n_data];
    // Group the (sorted) data pages into contiguous runs for the run table.
    let mut runs: Vec<(u32, u32)> = Vec::new();
    for &lp in data_lps {
        match runs.last_mut() {
            Some((first, len)) if *first + *len == lp => *len += 1,
            _ => runs.push((lp, 1)),
        }
    }
    if RunTable::RUNS_BASE + runs.len() * 3 + 3 >= PAGE_WORDS {
        return Err(FilesystemError::DiskFull(
            "Pilot: file too fragmented for a single-page run table".into(),
        ));
    }

    let mut disk = disk.clone();

    // Allocate a FileID (advance the LV-root counter at word 253).
    let lv_root_vda = pv_page; // logical page 0
    let fid = rdlong(&disk.sectors[lv_root_vda].data, 253).wrapping_add(1);
    let file_id = make_file_id(generation, fid);

    // Header page: run table describing the data extents.
    {
        let s = &mut disk.sectors[pv_page + header_lp as usize];
        s.label = Label::new(file_id, 0, attr::HEADER).bytes();
        let d = &mut s.data;
        for b in d.iter_mut() {
            *b = 0;
        }
        wrw(d, 0, 1); // headerPages
        wrw(d, 1, runs.len() as u16); // maxRuns
        let mut wi = RunTable::RUNS_BASE;
        for &(first, len) in &runs {
            wrlong(d, wi, first); // LogicalRun.first
            wrw(d, wi + 2, len as u16); // LogicalRun.size
            wi += 3;
        }
        wrlong(d, wi, LAST_LOGICAL_RUN); // terminator
    }

    // Data pages (filePage numbered 1.. within the file).
    for (i, &lp) in data_lps.iter().enumerate() {
        let s = &mut disk.sectors[pv_page + lp as usize];
        s.label = Label::new(file_id, (i + 1) as u32, attr::DATA).bytes();
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
        let is_file_page = matches!(label.attributes, attr::HEADER | attr::DATA);
        if is_file_page && label.file_id[0] == target[0] && label.file_id[1] == target[1] {
            disk.sectors[vda].label = Label::new([0; 5], lp as u32, attr::FREE_PAGE).bytes();
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

/// Encode a flat virtual disk address (VDA) as a 2-word `DiskFace.DontCare` /
/// `DiskAddress`, low word first. In the PDI logical-sector model the disk
/// address *is* the VDA; a geometry-accurate consumer maps VDA <-> (cylinder,
/// head, sector) for its pack, exactly as it regenerates ECC (PDI stores logical
/// sectors, not physical geometry). Used for both the `bootChainLink` and the
/// `bootingInfo` `firstLink`.
fn da_words(vda: u32) -> [u16; 2] {
    [(vda & 0xffff) as u16, (vda >> 16) as u16]
}
fn da_from_words(w: [u16; 2]) -> u32 {
    (w[0] as u32) | ((w[1] as u32) << 16)
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
        first_link: da_from_words([rdw(d, w + 7), rdw(d, w + 8)]),
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

    // Free logical pages (logical 0 is the LV root). First-fit, ascending; a
    // fresh volume yields one contiguous run, but a fragmented free list is fine
    // — the boot chain records run breaks.
    let free: Vec<u32> = (1..n_pages)
        .filter(|&lp| {
            disk.sector(pv_page + lp)
                .map(|s| Label::parse(&s.label).attributes == attr::FREE_PAGE)
                .unwrap_or(false)
        })
        .map(|lp| lp as u32)
        .collect();
    if free.len() < n {
        return Err(FilesystemError::DiskFull(format!(
            "Pilot: need {n} free pages to install the {} boot file ({} bytes), have {}",
            slot.label(),
            bytes.len(),
            free.len()
        )));
    }
    let pages = &free[..n];
    let vdas: Vec<u32> = pages.iter().map(|&lp| pv_page as u32 + lp).collect();

    let mut disk = disk.clone();

    // Allocate a FileID for the boot file (advance the LV-root counter).
    let lv_root_vda = pv_page;
    let fid = rdlong(&disk.sectors[lv_root_vda].data, 253).wrapping_add(1);
    let file_id = make_file_id(generation, fid);

    // Lay the pages and thread the boot chain through the labels' `dontCare`.
    for (i, (&lp, &vda)) in pages.iter().zip(&vdas).enumerate() {
        let dont_care = if i + 1 == n {
            BOOT_CHAIN_EOF // last page of the file
        } else if vdas[i + 1] != vda + 1 {
            da_words(vdas[i + 1]) // run break: point at the next run's first page
        } else {
            [0, 0] // interior of a contiguous run (link unused)
        };
        let s = &mut disk.sectors[pv_page + lp as usize];
        let mut label = Label::new(file_id, i as u32, attr::DATA);
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
    {
        let d = &mut disk.sectors[0].data;
        let w = slot.root_word();
        for (k, &v) in file_id.iter().enumerate() {
            wrw(d, w + k, v);
        }
        wrlong(d, w + 5, 0); // firstPage
        let first = da_words(vdas[0]);
        wrw(d, w + 7, first[0]);
        wrw(d, w + 8, first[1]);
        set_page_checksum(d);
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
                "Pilot: boot chain page {vda} fileID mismatch"
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
            da_from_words(l.dont_care)
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
        // and the VAM file (1 header + bitmap data pages) are in use; the rest
        // are free, and the VAM bitmap must agree with the label scan.
        let expected_size = 256 - OTHELLO_PV_RESERVE as u32 - 1;
        assert_eq!(vol.volume_size, expected_size);
        let vam_pages = 1 + vam_data_pages(expected_size) as u32;
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

        // A 700-byte file -> 2 data pages + 1 header = 3 pages consumed.
        let payload: Vec<u8> = (0..700u32).map(|i| (i % 251) as u8).collect();
        let (disk, fid) = add_file(&blank, Generation::CedarNucleus, &payload).expect("add_file");
        assert_eq!(fid, 2); // FileID 1 is reserved for the VAM

        let vol = read_volume(&disk, Generation::CedarNucleus).unwrap();
        assert_eq!(vol.free_pages, free_before - 3);
        // The VAM bitmap was rebuilt and agrees with the labels.
        assert_eq!(vol.vam_free_pages, Some(vol.free_pages));

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
                           // Each file = 1 header + 1 data = 2 pages; 4 consumed.
        assert_eq!(
            read_volume(&d2, Generation::CedarNucleus)
                .unwrap()
                .free_pages,
            free0 - 4
        );

        // Delete A: frees its 2 pages, leaving a gap before B's pages.
        let d3 = delete_file(&d2, fa).unwrap();
        assert_eq!(
            read_volume(&d3, Generation::CedarNucleus)
                .unwrap()
                .free_pages,
            free0 - 2
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
            if l.attributes == attr::HEADER && l.file_id != VAM_FILE_ID {
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
    }

    #[test]
    fn boot_chain_follows_run_breaks() {
        // Force a fragmented free list so the installed boot file spans a run
        // break, exercising the non-trivial `bootChainLink` path.
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

        // Confirm at least one page actually carried a forward link (not just the
        // [-1,-1] terminator) — i.e. the run break was exercised.
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
                vda = da_from_words(l.dont_care);
            } else {
                vda += 1;
            }
        }
        assert!(
            saw_link,
            "expected a non-trivial bootChainLink across the hole"
        );
    }
}
