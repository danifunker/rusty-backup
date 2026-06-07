//! CP/M Disk Parameter Block (DPB) presets keyed by MiSTer core.
//!
//! Every CP/M filesystem implementation needs a DPB describing the
//! disk's geometry — without one, the directory location and allocation-
//! block size are unguessable. Real CP/M boots ship a DPB in their BIOS
//! and never expose it to the filesystem layer; we have to carry our
//! own registry (the cpmtools project does the same thing, calling it
//! `diskdefs`).
//!
//! Each preset below is the DPB that the matching MiSTer-core's BIOS
//! uses, with provenance noted inline. The plan
//! ([`docs/mister_filesystem_implementation_plan.md`] §3.4 / §2.3)
//! mandates seeding these per-machine; if a new core surfaces, add a
//! preset here rather than passing magic numbers from a call site.
//!
//! Field semantics are the standard CP/M BIOS Disk Parameter Block:
//!
//! ```text
//! spt   records per track (128-B records)
//! bsh   block shift — block size = 128 << bsh
//! blm   block mask  = (block_size / 128) - 1
//! exm   extent mask — how many physical extents share one dir entry
//! dsm   number of allocation blocks - 1 (so total blocks = dsm + 1)
//! drm   number of directory entries - 1
//! al0   bitmap byte: blocks holding the directory (MSB = block 0)
//! al1   bitmap byte 2 (covers blocks 8..15)
//! cks   directory checksum size (for removable media tracking)
//! off   reserved track count at the start of the disk
//! psh   physical sector shift (1 byte; CP/M 3 only, optional)
//! phm   physical sector mask (1 byte; CP/M 3 only, optional)
//! ```

/// Parsed Disk Parameter Block — the on-DPB-record field shapes plus
/// derived fields the FS uses every-where.
#[derive(Debug, Clone, Copy)]
pub struct Dpb {
    pub name: &'static str,
    pub spt: u16,
    pub bsh: u8,
    pub blm: u8,
    pub exm: u8,
    pub dsm: u16,
    pub drm: u16,
    pub al0: u8,
    pub al1: u8,
    pub cks: u16,
    pub off: u16,
    pub psh: u8,
    pub phm: u8,
    /// Logical sector size in bytes (almost always 128 in CP/M 2.2
    /// images; some 3.0 disks have a non-128 phys sector layout that
    /// CP/M maps down to 128-B records via `psh`/`phm`).
    pub sector_size: u16,
    /// Number of tracks on the disk. Some presets cap this at 80; the
    /// FS uses it for sanity bounds-checking only.
    pub tracks: u16,
}

impl Dpb {
    pub const fn block_size(&self) -> usize {
        128usize << (self.bsh as usize)
    }

    /// Total addressable allocation blocks.
    pub const fn total_blocks(&self) -> u32 {
        self.dsm as u32 + 1
    }

    /// Maximum number of directory entries.
    pub const fn max_dir_entries(&self) -> u32 {
        self.drm as u32 + 1
    }

    /// Number of 128-B records per allocation block.
    pub const fn records_per_block(&self) -> u32 {
        self.blm as u32 + 1
    }

    /// Total disk capacity in bytes (data area only, excluding reserved
    /// tracks).
    pub const fn data_bytes(&self) -> u64 {
        self.total_blocks() as u64 * self.block_size() as u64
    }

    /// True if directory entries use 16-bit allocation block pointers
    /// (i.e. there are > 256 blocks). For ≤ 256 blocks, pointers are
    /// 8-bit. This drives how `parse_dir_entry` reads bytes 16..31.
    pub const fn uses_word_pointers(&self) -> bool {
        self.dsm >= 256
    }
}

/// Amstrad CPC 6128 system-format disk (CP/M Plus side, 9 sectors per
/// track of 512 B, single-sided 80-track but laid out double-density;
/// the BIOS skips the first 2 tracks for the boot sector). Default
/// for `.dsk` files dropped into `/media/fat/games/Amstrad/`.
pub const AMSTRAD_SYS: Dpb = Dpb {
    name: "amstrad_sys",
    spt: 36, // 9 phys × 512 / 128
    bsh: 3,
    blm: 7,
    exm: 0,
    dsm: 170,
    drm: 63,
    al0: 0xC0,
    al1: 0x00,
    cks: 16,
    off: 2,
    psh: 2,
    phm: 3,
    sector_size: 512,
    tracks: 40,
};

/// Amstrad CPC 6128 data-format disk (same geometry, no reserved
/// tracks — used for blank user disks).
pub const AMSTRAD_DATA: Dpb = Dpb {
    name: "amstrad_data",
    spt: 36,
    bsh: 3,
    blm: 7,
    exm: 0,
    dsm: 179,
    drm: 63,
    al0: 0xC0,
    al1: 0x00,
    cks: 16,
    off: 0,
    psh: 2,
    phm: 3,
    sector_size: 512,
    tracks: 40,
};

/// Amstrad PCW Format A — the dominant PCW floppy in TOSEC: single-sided
/// 40-track, 9 × 512 B sectors, 1 reserved (boot) track. Matches cpmtools'
/// `pcw` diskdef and libdsk `pcw180`. Used for app and system disks on the
/// original PCW 8256/8512 (the "CF2" 3" media). Format B (706 KB 80-track
/// double-sided) would be a separate preset once a fixture surfaces.
pub const AMSTRAD_PCW: Dpb = Dpb {
    name: "amstrad_pcw",
    spt: 36,
    bsh: 3,
    blm: 7,
    exm: 0,
    dsm: 174,
    drm: 63,
    al0: 0xC0,
    al1: 0x00,
    cks: 16,
    off: 1,
    psh: 2,
    phm: 3,
    sector_size: 512,
    tracks: 40,
};

/// Tatung Einstein 80T DS 10×512 — the canonical Einstein floppy.
pub const EINSTEIN: Dpb = Dpb {
    name: "einstein",
    spt: 40,
    bsh: 4,
    blm: 15,
    exm: 1,
    dsm: 199,
    drm: 127,
    al0: 0xC0,
    al1: 0x00,
    cks: 32,
    off: 2,
    psh: 2,
    phm: 3,
    sector_size: 512,
    tracks: 80,
};

/// Spectravideo SV-328 CP/M 80 KB SS 40-track (17 phys × 256 B).
pub const SVI328_CPM: Dpb = Dpb {
    name: "svi328_cpm",
    spt: 34,
    bsh: 4,
    blm: 15,
    exm: 1,
    dsm: 153,
    drm: 63,
    al0: 0xC0,
    al1: 0x00,
    cks: 16,
    off: 2,
    psh: 1,
    phm: 1,
    sector_size: 256,
    tracks: 40,
};

/// MITS Altair 8800 8" SSSD floppy — the original CP/M target. 26
/// sectors of 128 B, 77 tracks, first 2 reserved.
pub const ALTAIR_8IN: Dpb = Dpb {
    name: "altair_8in",
    spt: 26,
    bsh: 3,
    blm: 7,
    exm: 0,
    dsm: 242,
    drm: 63,
    al0: 0xC0,
    al1: 0x00,
    cks: 16,
    off: 2,
    psh: 0,
    phm: 0,
    sector_size: 128,
    tracks: 77,
};

/// Altair 8800 CF disk (Altair-Z80 era). Sectorized as 512 B in 8 MB
/// chunks; the MiSTer Altair8800 core supports IDE-style CF images.
pub const ALTAIR_CF: Dpb = Dpb {
    name: "altair_cf",
    spt: 64, // 8 phys × 512 / 128 — adjust per the CF size you boot with
    bsh: 4,
    blm: 15,
    exm: 0,
    dsm: 2047,
    drm: 1023,
    al0: 0xFF,
    al1: 0x00,
    cks: 0,
    off: 0,
    psh: 2,
    phm: 3,
    sector_size: 512,
    tracks: 0,
};

/// MultiComp standard SD card image (Grant Searle's project; MiSTer
/// MultiComp core defaults). 80 trk × 10 spt × 512 B, SS.
pub const MULTICOMP: Dpb = Dpb {
    name: "multicomp",
    spt: 40,
    bsh: 4,
    blm: 15,
    exm: 1,
    dsm: 199,
    drm: 127,
    al0: 0xC0,
    al1: 0x00,
    cks: 32,
    off: 2,
    psh: 2,
    phm: 3,
    sector_size: 512,
    tracks: 80,
};

/// ZX-Spectrum +3 / +3DOS — Amstrad data-format-compatible.
pub const ZX_PLUS3: Dpb = Dpb {
    name: "zxplus3",
    spt: 36,
    bsh: 3,
    blm: 7,
    exm: 0,
    dsm: 179,
    drm: 63,
    al0: 0xC0,
    al1: 0x00,
    cks: 16,
    off: 1,
    psh: 2,
    phm: 3,
    sector_size: 512,
    tracks: 40,
};

/// All shipped presets, in the order a heuristic detector should try.
pub const ALL_PRESETS: &[Dpb] = &[
    AMSTRAD_DATA,
    AMSTRAD_SYS,
    AMSTRAD_PCW,
    EINSTEIN,
    SVI328_CPM,
    ALTAIR_8IN,
    ALTAIR_CF,
    MULTICOMP,
    ZX_PLUS3,
];

/// Look up a preset by name. Returns `None` if the name is unknown.
pub fn preset_by_name(name: &str) -> Option<&'static Dpb> {
    ALL_PRESETS
        .iter()
        .find(|d| d.name.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_presets_have_sane_field_relationships() {
        for d in ALL_PRESETS {
            // blm = (block_size / 128) - 1
            assert_eq!(
                d.blm as u32 + 1,
                d.block_size() as u32 / 128,
                "preset {} BLM mismatches BSH",
                d.name
            );
            // block size must be a power of two between 128 and 16384.
            let bs = d.block_size();
            assert!(
                (128..=16384).contains(&bs) && bs.is_power_of_two(),
                "preset {} has weird block size {bs}",
                d.name
            );
            // dsm + 1 must be > 0.
            assert!(d.total_blocks() > 0, "preset {} has 0 total blocks", d.name);
        }
    }

    #[test]
    fn preset_lookup_round_trip() {
        for d in ALL_PRESETS {
            let looked = preset_by_name(d.name).expect("lookup");
            assert_eq!(looked.name, d.name);
        }
        assert!(preset_by_name("nonexistent").is_none());
    }
}
