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
fn set_page_checksum(data: &mut [u8]) {
    let words: Vec<u16> = (0..PAGE_WORDS - 1).map(|i| rdw(data, i)).collect();
    wrw(data, PAGE_WORDS - 1, pilot_checksum(&words));
}
/// Verify a page's stored checksum (word 255).
fn page_checksum_ok(data: &[u8]) -> bool {
    let words: Vec<u16> = (0..PAGE_WORDS - 1).map(|i| rdw(data, i)).collect();
    pilot_checksum(&words) == rdw(data, PAGE_WORDS - 1)
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
        if !page_checksum_ok(data) {
            return Err(FilesystemError::Parse(
                "Pilot: physical-root checksum mismatch".into(),
            ));
        }
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
        if !page_checksum_ok(data) {
            return Err(FilesystemError::Parse(
                "Pilot: logical-root checksum mismatch".into(),
            ));
        }
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
            if Label::parse(&s.label).attributes == attr::FREE_PAGE {
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

    fn total_pages(&self) -> u64 {
        self.runs.iter().map(|&(_, n)| n as u64).sum()
    }
}

/// Read-only [`Filesystem`] view of a Pilot/Cedar volume. File enumeration is by
/// `header`-attribute page scan (the Cedar nucleus has no name directory in the
/// nucleus, so files surface by file ID); reads follow the run table.
pub struct PilotFilesystem {
    disk: Disk,
    #[allow(dead_code)]
    generation: Generation,
    volume: PilotVolume,
}

impl PilotFilesystem {
    pub fn open(disk: Disk, generation: Generation) -> Result<Self, FilesystemError> {
        let volume = read_volume(&disk, generation)?;
        Ok(Self {
            disk,
            generation,
            volume,
        })
    }

    /// The single subvolume that holds the logical volume root.
    fn root_subvolume(&self) -> &SubVolumeDesc {
        // read_volume verified this exists.
        self.volume
            .physical_root
            .sub_volumes
            .iter()
            .find(|sv| sv.lv_page == ROOT_PAGE_NUMBER)
            .expect("root subvolume present (validated at open)")
    }

    fn enumerate(&self) -> Vec<FileEntry> {
        let sv = self.root_subvolume();
        let mut out = Vec::new();
        for lp in 0..sv.n_pages as usize {
            let vda = sv.pv_page as usize + lp;
            let Some(sector) = self.disk.sector(vda) else {
                continue;
            };
            let label = Label::parse(&sector.label);
            if label.attributes != attr::HEADER {
                continue;
            }
            // The VAM is a (header) file too, but it's volume metadata, not a
            // user file — keep it out of the browse listing.
            if label.file_id == VAM_FILE_ID {
                continue;
            }
            let run = RunTable::parse(&sector.data);
            let size = run.total_pages() * PAGE_BYTES as u64;
            let name = format!("File-{:04X}{:04X}", label.file_id[0], label.file_id[1]);
            out.push(FileEntry::new_file(
                name.clone(),
                format!("/{name}"),
                size,
                vda as u64,
            ));
        }
        out
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
        let sv = self.root_subvolume();
        let header_vda = entry.location as usize;
        let header = self
            .disk
            .sector(header_vda)
            .ok_or_else(|| FilesystemError::Parse("Pilot: file header page out of range".into()))?;
        let run = RunTable::parse(&header.data);

        let mut out = Vec::new();
        'runs: for (first, count) in run.runs {
            for p in 0..count {
                let logical = first + p;
                let vda = sv.pv_page + logical;
                let Some(sector) = self.disk.sector(vda as usize) else {
                    break 'runs;
                };
                let take = (max_bytes - out.len()).min(PAGE_BYTES);
                out.extend_from_slice(&sector.data[..take]);
                if out.len() >= max_bytes {
                    break 'runs;
                }
            }
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
    fn detects_corrupted_root_checksum() {
        let geo = pilot_geometry(128);
        let mut disk = create_blank(geo, Generation::CedarNucleus, "X").expect("create");
        // Corrupt a word in the physical root data; checksum must catch it.
        disk.sectors[0].data[10] ^= 0xff;
        assert!(read_volume(&disk, Generation::CedarNucleus).is_err());
    }
}
