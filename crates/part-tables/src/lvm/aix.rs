//! AIX LVM (AIX 3.x-7.x volume groups), read-only, single physical volume.
//!
//! Every AIX disk is an LVM physical volume. Sector 0 holds the IBM IPL record
//! (EBCDIC `IBMA`, `0xC9C2D4C1`); PowerPC machines also put a PReP-style MBR there.
//! Sector 7 holds the LVM record. Everything is big-endian and counted in 512-byte
//! sectors. Layout facts come from Linux's `block/partitions/aix.c` (format
//! documentation only) and were checked against an AIX 4.3.3 rootvg.
//!
//! LVM record (sector 7, magic `_LVM`): VGDA length at 24, the two VGDA copies'
//! sectors at 28 and 32, `log2` of the physical-partition size at 46, version (1)
//! at 60.
//!
//! VGDA (at its first sector): timestamp (`u32` seconds, `u32` microseconds) at 0,
//! `u16` LV count at 24, `u16` maximum LVs at 26, `u16` PV count at 30. The LV
//! descriptors follow at sector +1, 32 bytes each, one per possible LV: `u16`
//! logical-partition count at 14 (`0` = unused). LV names are 64-byte strings
//! starting 33 sectors before the VGDA's end. The PV descriptor is at sector +17:
//! `u16` PP count at 16, sector of PP 0 at 20, then one 32-byte entry per
//! physical partition from 32: `u16` LV index (1-based) at 0, `u16` logical
//! partition (1-based, `0` = free) at 6. A mirrored LP shows up more than once;
//! the first copy is used.

use std::io::{Read, Seek, SeekFrom};

use serde::{Deserialize, Serialize};

use super::LogicalVolume;

/// IBM IPL record magic at sector 0: `IBMA` in EBCDIC.
pub const IPL_MAGIC: [u8; 4] = [0xC9, 0xC2, 0xD4, 0xC1];
const SECTOR: u64 = 512;
const LVM_RECORD_SECTOR: u64 = 7;
const LVD_BYTES: usize = 32;
const PPE_BYTES: usize = 32;
/// `struct pvd` holds at most this many partition entries.
const MAX_PPS: usize = 1016;

/// A single-PV AIX volume group as seen from this disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AixLvm {
    /// Byte offset of physical partition 0.
    pub pp_start: u64,
    pub pp_size: u64,
    pub pp_count: u32,
    /// Physical volumes the VGDA lists; more than one means a multi-disk group.
    pub pv_count: u32,
    /// `(LV number, volume)` for every LV in use, named from the VGDA (`hd4`, `hd2`, ...).
    pub volumes: Vec<(u32, LogicalVolume)>,
    /// Sector 0 carries the IBM IPL record.
    pub has_ipl: bool,
}

impl AixLvm {
    /// Parse the LVM record and the newer VGDA copy, or `None` when the disk has none.
    pub fn detect<R: Read + Seek>(reader: &mut R) -> Option<Self> {
        let mut s0 = [0u8; 4];
        reader.seek(SeekFrom::Start(0)).ok()?;
        reader.read_exact(&mut s0).ok()?;
        let mut rec = [0u8; 512];
        reader
            .seek(SeekFrom::Start(LVM_RECORD_SECTOR * SECTOR))
            .ok()?;
        reader.read_exact(&mut rec).ok()?;
        if &rec[0..4] != b"_LVM" || be16(&rec, 60) != 1 {
            return None;
        }
        let vgda_len = be32(&rec, 24) as u64;
        let shift = be16(&rec, 46) as u32;
        if !(20..=30).contains(&shift) || vgda_len < 64 {
            return None;
        }
        let pp_size = 1u64 << shift;
        let vgda_sector = [be32(&rec, 28), be32(&rec, 32)]
            .iter()
            .filter(|&&s| s != 0)
            .filter_map(|&s| {
                let mut h = [0u8; 8];
                reader.seek(SeekFrom::Start(s as u64 * SECTOR)).ok()?;
                reader.read_exact(&mut h).ok()?;
                Some(((be32(&h, 0), be32(&h, 4)), s as u64))
            })
            .max()?
            .1;
        let mut vgda = vec![0u8; (vgda_len * SECTOR) as usize];
        reader.seek(SeekFrom::Start(vgda_sector * SECTOR)).ok()?;
        reader.read_exact(&mut vgda).ok()?;
        Self::from_vgda(&vgda, pp_size, s0 == IPL_MAGIC)
    }

    fn from_vgda(vgda: &[u8], pp_size: u64, has_ipl: bool) -> Option<Self> {
        let max_lvs = be16(vgda, 26) as usize;
        let pv_count = be16(vgda, 30) as u32;
        let names_at = vgda.len().checked_sub(33 * SECTOR as usize)?;
        let pvd = 17 * SECTOR as usize;
        if max_lvs == 0 || pvd + 32 > vgda.len() || names_at + max_lvs * 64 > vgda.len() {
            return None;
        }
        let pp_count = (be16(vgda, pvd + 16) as usize).min(MAX_PPS);
        let pp_start = be32(vgda, pvd + 20) as u64 * SECTOR;
        if pvd + 32 + pp_count * PPE_BYTES > vgda.len() {
            return None;
        }
        let mut found: Vec<Vec<(u64, u64)>> = vec![Vec::new(); max_lvs + 1];
        for pp in 0..pp_count {
            let e = pvd + 32 + pp * PPE_BYTES;
            let (lv, lp) = (be16(vgda, e) as usize, be16(vgda, e + 6) as u64);
            if lp != 0 && (1..=max_lvs).contains(&lv) {
                found[lv].push((lp - 1, pp_start + pp as u64 * pp_size));
            }
        }
        let volumes = (1..=max_lvs)
            .filter_map(|lv| {
                let lps = be16(vgda, SECTOR as usize + (lv - 1) * LVD_BYTES + 14) as u64;
                (lps != 0).then(|| {
                    let raw = &vgda[names_at + (lv - 1) * 64..names_at + lv * 64];
                    let name =
                        String::from_utf8_lossy(raw.split(|&b| b == 0).next().unwrap_or(&[]))
                            .trim()
                            .to_string();
                    let name = if name.is_empty() {
                        format!("lv{lv}")
                    } else {
                        name
                    };
                    let vol = LogicalVolume::from_extents(
                        name,
                        lps,
                        pp_size,
                        std::mem::take(&mut found[lv]),
                    );
                    (lv as u32, vol)
                })
            })
            .collect();
        Some(Self {
            pp_start,
            pp_size,
            pp_count: pp_count as u32,
            pv_count,
            volumes,
            has_ipl,
        })
    }
}

fn be16(b: &[u8], o: usize) -> u16 {
    u16::from_be_bytes([b[o], b[o + 1]])
}

fn be32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A small rootvg: 1 MiB PPs from sector 2048, `hd4` split across PPs 0 and 2, `hd5` on PP 1.
    fn sample_disk() -> Vec<u8> {
        let mut d = vec![0u8; 8 << 20];
        d[0..4].copy_from_slice(&IPL_MAGIC);
        let r = 7 * 512;
        d[r..r + 4].copy_from_slice(b"_LVM");
        let vgda_len = 100u32;
        d[r + 24..r + 28].copy_from_slice(&vgda_len.to_be_bytes());
        d[r + 28..r + 32].copy_from_slice(&200u32.to_be_bytes());
        d[r + 46..r + 48].copy_from_slice(&20u16.to_be_bytes());
        d[r + 60..r + 62].copy_from_slice(&1u16.to_be_bytes());
        let v = 200 * 512;
        d[v + 26..v + 28].copy_from_slice(&4u16.to_be_bytes()); // max LVs
        d[v + 30..v + 32].copy_from_slice(&1u16.to_be_bytes()); // one PV
        let lvd = |d: &mut Vec<u8>, lv: usize, lps: u16| {
            let o = v + 512 + (lv - 1) * 32 + 14;
            d[o..o + 2].copy_from_slice(&lps.to_be_bytes());
        };
        lvd(&mut d, 1, 1);
        lvd(&mut d, 2, 2);
        let names = v + (vgda_len as usize - 33) * 512;
        d[names..names + 3].copy_from_slice(b"hd5");
        d[names + 64..names + 67].copy_from_slice(b"hd4");
        let pvd = v + 17 * 512;
        d[pvd + 16..pvd + 18].copy_from_slice(&3u16.to_be_bytes());
        d[pvd + 20..pvd + 24].copy_from_slice(&2048u32.to_be_bytes());
        for (pp, (lv, lp)) in [(2u16, 1u16), (1, 1), (2, 2)].iter().enumerate() {
            let e = pvd + 32 + pp * 32;
            d[e..e + 2].copy_from_slice(&lv.to_be_bytes());
            d[e + 6..e + 8].copy_from_slice(&lp.to_be_bytes());
        }
        d
    }

    #[test]
    fn maps_named_lvs_including_a_fragmented_one() {
        let lvm = AixLvm::detect(&mut Cursor::new(sample_disk())).expect("aix lvm");
        assert!(lvm.has_ipl);
        assert_eq!(
            (lvm.pp_start, lvm.pp_size, lvm.pp_count),
            (1 << 20, 1 << 20, 3)
        );
        let hd5 = &lvm.volumes[0].1;
        assert_eq!(
            (hd5.name.as_str(), hd5.contiguous_start()),
            ("hd5", Some(2 << 20))
        );
        let hd4 = &lvm.volumes[1].1;
        assert_eq!(hd4.name, "hd4");
        assert!(hd4.complete);
        assert_eq!(hd4.runs, vec![(1 << 20, 1 << 20), (3 << 20, 1 << 20)]);
    }

    #[test]
    fn no_lvm_record_is_none() {
        assert!(AixLvm::detect(&mut Cursor::new(vec![0u8; 1 << 20])).is_none());
    }
}
