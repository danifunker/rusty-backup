//! Solaris x86 VTOC — the slice table Solaris/x86 nests **inside** an MBR
//! partition, rather than replacing the MBR the way SPARC's Sun disk label
//! does.
//!
//! The disk carries an ordinary PC MBR whose Solaris entry is type `0x82`
//! (`SUNIXOS`, Solaris 2.x through 9) or `0xBF` (`SUNIXOS2`, Solaris 10+).
//! Sector 1 of that partition holds a `struct vtoc` — little-endian, ILP32,
//! and unrelated to the big-endian 512-byte SPARC label in
//! [`crate::partition::sun`]:
//!
//! - `v_bootinfo[3]` @0, `v_sanity` @12 (`0x600DDEEE`), `v_version` @16,
//! - `v_volume[8]` @20, `v_sectorsz` @28, `v_nparts` @30,
//! - `v_reserved[10]` @32, `v_part[16]` @72 — each
//!   `{p_tag u16, p_flag u16, p_start u32, p_size u32}`,
//! - `timestamp[16]` @264, `v_asciilabel[128]` @328.
//!
//! **Slice offsets are relative to the Solaris MBR partition**, not the disk,
//! so a slice's absolute start is `mbr_entry.start_lba + p_start`. Type byte
//! `0x82` also means "Linux swap" on a PC, so nothing here fires without a
//! matching `v_sanity`, `v_version == 1`, a sane `v_nparts`, and slices that
//! fit inside the partition.

use byteorder::{ByteOrder, LittleEndian};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};

use super::mbr::Mbr;
use super::sun::{tag_name, SUN_TAG_WHOLE_DISK};
use crate::error::RustyBackupError;

/// `VTOC_SANE` — the sanity word at offset 12.
pub const VTOC_SANITY: u32 = 0x600D_DEEE;
/// `SUNIXOS` — Solaris 2.x .. 9 on x86. Shared with "Linux swap".
pub const MBR_TYPE_SUNIXOS: u8 = 0x82;
/// `SUNIXOS2` — Solaris 10+ on x86.
pub const MBR_TYPE_SUNIXOS2: u8 = 0xBF;

/// `V_NUMPAR` — slots in the VTOC.
pub const N_SLICES: usize = 16;

const VTOC_OFF_SANITY: usize = 12;
const VTOC_OFF_VERSION: usize = 16;
const VTOC_OFF_VOLUME: usize = 20;
const VTOC_OFF_SECTORSZ: usize = 28;
const VTOC_OFF_NPARTS: usize = 30;
const VTOC_OFF_PART: usize = 72;
const VTOC_OFF_ASCIILABEL: usize = 328;
const PART_ENTRY_SIZE: usize = 12;
/// Bytes of the sector we need; a VTOC is 456 bytes and lives in one sector.
const VTOC_SIZE: usize = 456;
/// The VTOC lives in sector 1 of the Solaris partition; sector 0 is `mboot`.
const VTOC_SECTOR: u64 = 1;

/// One Solaris x86 slice, resolved to absolute sectors.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SolarisSlice {
    /// `p_tag` — same vocabulary as the SPARC VTOC (`root`, `swap`, `usr`, …).
    pub tag: u16,
    /// `p_flag` — bit 0 unmountable, bit 1 read-only.
    pub flags: u16,
    /// `p_start` as stored: sectors from the start of the Solaris partition.
    pub relative_start: u32,
    /// Absolute start sector on the disk.
    pub start_sector: u64,
    /// `p_size` in 512-byte sectors.
    pub num_sectors: u32,
}

impl SolarisSlice {
    pub fn is_empty(&self) -> bool {
        self.num_sectors == 0
    }
    /// Slice 2 conventionally spans the whole Solaris partition and overlaps
    /// every real slice, so it is skipped from the browse list.
    pub fn is_whole_disk(&self) -> bool {
        self.tag == SUN_TAG_WHOLE_DISK
    }
    pub fn size_bytes(&self) -> u64 {
        self.num_sectors as u64 * 512
    }
    pub fn tag_name(&self) -> &'static str {
        tag_name(self.tag)
    }
}

/// A parsed Solaris x86 VTOC plus the MBR that hosts it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolarisX86Label {
    /// `v_volume`, when set.
    pub volume: String,
    /// `v_asciilabel` — the geometry string `format(1M)` prints.
    pub ascii_label: String,
    pub version: u32,
    /// `v_sectorsz`; 512 on every disk we have seen.
    pub sector_size: u16,
    pub nparts: u16,
    /// MBR slot (0-3) the Solaris partition occupies.
    pub mbr_slot: usize,
    /// Absolute start of the Solaris MBR partition, in 512-byte sectors.
    pub partition_start_lba: u64,
    /// Length of the Solaris MBR partition, in 512-byte sectors.
    pub partition_sectors: u64,
    pub slices: Vec<SolarisSlice>,
}

impl SolarisX86Label {
    /// Non-empty slices in slice order, skipping the whole-partition alias.
    pub fn browsable_slices(&self) -> impl Iterator<Item = (usize, &SolarisSlice)> {
        self.slices
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.is_empty() && !s.is_whole_disk())
    }

    /// Parse one VTOC sector. `partition_start_lba` resolves slice offsets.
    pub fn parse(
        buf: &[u8],
        mbr_slot: usize,
        partition_start_lba: u64,
        partition_sectors: u64,
    ) -> Result<Self, RustyBackupError> {
        if buf.len() < VTOC_SIZE {
            return Err(RustyBackupError::InvalidMbr(
                "Solaris x86 VTOC: buffer shorter than one VTOC".into(),
            ));
        }
        if LittleEndian::read_u32(&buf[VTOC_OFF_SANITY..VTOC_OFF_SANITY + 4]) != VTOC_SANITY {
            return Err(RustyBackupError::InvalidMbr(
                "Solaris x86 VTOC: bad sanity word".into(),
            ));
        }
        let nparts = LittleEndian::read_u16(&buf[VTOC_OFF_NPARTS..VTOC_OFF_NPARTS + 2]);
        let count = (nparts as usize).min(N_SLICES);

        let mut slices = Vec::with_capacity(count);
        for i in 0..count {
            let base = VTOC_OFF_PART + i * PART_ENTRY_SIZE;
            let relative_start = LittleEndian::read_u32(&buf[base + 4..base + 8]);
            slices.push(SolarisSlice {
                tag: LittleEndian::read_u16(&buf[base..base + 2]),
                flags: LittleEndian::read_u16(&buf[base + 2..base + 4]),
                relative_start,
                start_sector: partition_start_lba + relative_start as u64,
                num_sectors: LittleEndian::read_u32(&buf[base + 8..base + 12]),
            });
        }

        Ok(SolarisX86Label {
            volume: c_string(&buf[VTOC_OFF_VOLUME..VTOC_OFF_VOLUME + 8]),
            ascii_label: c_string(&buf[VTOC_OFF_ASCIILABEL..VTOC_OFF_ASCIILABEL + 128]),
            version: LittleEndian::read_u32(&buf[VTOC_OFF_VERSION..VTOC_OFF_VERSION + 4]),
            sector_size: LittleEndian::read_u16(&buf[VTOC_OFF_SECTORSZ..VTOC_OFF_SECTORSZ + 2]),
            nparts,
            mbr_slot,
            partition_start_lba,
            partition_sectors,
            slices,
        })
    }
}

/// Look for a Solaris VTOC inside any Solaris-typed MBR partition.
pub fn detect<R: Read + Seek>(reader: &mut R, mbr: &Mbr) -> Option<SolarisX86Label> {
    let disk_size = reader.seek(SeekFrom::End(0)).ok()?;
    for (slot, entry) in mbr.entries.iter().enumerate() {
        if entry.is_empty() || !matches!(entry.partition_type, MBR_TYPE_SUNIXOS | MBR_TYPE_SUNIXOS2)
        {
            continue;
        }
        let start_lba = entry.start_lba as u64;
        let sectors = entry.size_bytes() / 512;
        let vtoc_at = (start_lba + VTOC_SECTOR) * 512;
        if vtoc_at + VTOC_SIZE as u64 > disk_size {
            continue;
        }
        if reader.seek(SeekFrom::Start(vtoc_at)).is_err() {
            continue;
        }
        let mut buf = [0u8; VTOC_SIZE];
        if reader.read_exact(&mut buf).is_err() {
            continue;
        }
        let label = match SolarisX86Label::parse(&buf, slot, start_lba, sectors) {
            Ok(l) => l,
            Err(_) => continue,
        };
        if validates(&label) {
            return Some(label);
        }
    }
    None
}

/// Type byte `0x82` is also Linux swap, so every structural field has to agree
/// before we reinterpret an ordinary PC disk as a Solaris one.
fn validates(label: &SolarisX86Label) -> bool {
    if label.version != 1 || label.nparts == 0 || label.nparts as usize > N_SLICES {
        return false;
    }
    if label.sector_size != 0 && label.sector_size != 512 {
        return false;
    }
    let mut live = 0usize;
    for slice in &label.slices {
        if slice.is_empty() {
            continue;
        }
        live += 1;
        let end = slice.relative_start as u64 + slice.num_sectors as u64;
        if end > label.partition_sectors {
            return false;
        }
    }
    live > 0
}

fn c_string(raw: &[u8]) -> String {
    let end = raw.iter().position(|&c| c == 0).unwrap_or(raw.len());
    String::from_utf8_lossy(&raw[..end]).trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synth_vtoc() -> [u8; VTOC_SIZE] {
        let mut b = [0u8; VTOC_SIZE];
        LittleEndian::write_u32(&mut b[VTOC_OFF_SANITY..VTOC_OFF_SANITY + 4], VTOC_SANITY);
        LittleEndian::write_u32(&mut b[VTOC_OFF_VERSION..VTOC_OFF_VERSION + 4], 1);
        LittleEndian::write_u16(&mut b[VTOC_OFF_SECTORSZ..VTOC_OFF_SECTORSZ + 2], 512);
        LittleEndian::write_u16(&mut b[VTOC_OFF_NPARTS..VTOC_OFF_NPARTS + 2], 16);
        b[VTOC_OFF_ASCIILABEL..VTOC_OFF_ASCIILABEL + 9].copy_from_slice(b"DEFAULT c");
        // slice 0 = root, slice 2 = whole-partition backup.
        let s0 = VTOC_OFF_PART;
        LittleEndian::write_u16(&mut b[s0..s0 + 2], 2);
        LittleEndian::write_u32(&mut b[s0 + 4..s0 + 8], 100);
        LittleEndian::write_u32(&mut b[s0 + 8..s0 + 12], 900);
        let s2 = VTOC_OFF_PART + 2 * PART_ENTRY_SIZE;
        LittleEndian::write_u16(&mut b[s2..s2 + 2], SUN_TAG_WHOLE_DISK);
        LittleEndian::write_u32(&mut b[s2 + 8..s2 + 12], 1000);
        b
    }

    #[test]
    fn slice_offsets_are_partition_relative() {
        let label = SolarisX86Label::parse(&synth_vtoc(), 0, 8064, 1000).unwrap();
        let live: Vec<_> = label.browsable_slices().collect();
        assert_eq!(live.len(), 1, "the whole-partition slice is skipped");
        assert_eq!(live[0].0, 0);
        assert_eq!(live[0].1.start_sector, 8064 + 100);
        assert_eq!(live[0].1.tag_name(), "root");
    }

    #[test]
    fn a_wrong_sanity_word_is_not_a_vtoc() {
        let mut b = synth_vtoc();
        b[VTOC_OFF_SANITY] ^= 0xFF;
        assert!(SolarisX86Label::parse(&b, 0, 0, 1000).is_err());
    }

    #[test]
    fn slices_running_past_the_partition_are_rejected() {
        let label = SolarisX86Label::parse(&synth_vtoc(), 0, 8064, 500).unwrap();
        assert!(!validates(&label));
    }

    #[test]
    fn version_two_labels_are_rejected() {
        let mut b = synth_vtoc();
        LittleEndian::write_u32(&mut b[VTOC_OFF_VERSION..VTOC_OFF_VERSION + 4], 2);
        let label = SolarisX86Label::parse(&b, 0, 8064, 1000).unwrap();
        assert!(!validates(&label));
    }
}
