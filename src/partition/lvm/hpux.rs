//! HP-UX LVM (HP-UX 10.x / 11.x volume groups), read-only, single physical volume.
//!
//! An HP-UX boot disk is a LIF boot volume at sector 0 (`ISL`, `HPUX`, `AUTO`)
//! followed by LVM metadata; the logical volumes live in physical extents after
//! that. Everything is big-endian and, apart from the extent size, counted in
//! 1 KiB sectors ("PSN"s). No public spec was used. The layout was worked out
//! from an HP-UX 11.11 PA-RISC disk and checked against the filesystems found in
//! its extents (the `/stand` HFS and the root VxFS).
//!
//! LVM record (byte 8192, magic `LVMREC01`): PV id at 8 and VG id at 16 (two
//! `u32` each), last PSN at 24, VGDA length (KiB) at 40, the two VGDA copies' PSNs
//! at 48 and 52, first physical extent PSN at 72, extent size as a power of two
//! (bytes) at 76 and in KiB at 80.
//!
//! VGDA (each copy): timestamp (`u32` seconds, `u32` microseconds) at 0, VG id at
//! 8, `u16` maximum LVs at 16, `u16` maximum extents per PV at 20. The newer of the
//! two valid copies wins. LV entries start at +1 KiB, 16 bytes each, one per
//! possible LV: `u16` logical-extent count at 0 (`0` = unused). The LV number is
//! its 1-based slot, named `lvolN`. PV entries follow, starting on the next KiB:
//! PV id (8 bytes), `u16` extent count at 8, then a map of `u16` LV number and
//! `u16` logical extent per physical extent from +16 (`0` = free).

use std::io::{Read, Seek, SeekFrom};

use serde::{Deserialize, Serialize};

use super::LogicalVolume;

const LVMREC_OFFSET: u64 = 8192;
const LVMREC_MAGIC: &[u8; 8] = b"LVMREC01";
const KIB: u64 = 1024;
/// HP-UX caps a VGDA well under this; guards the read against a corrupt length.
const MAX_VGDA_KIB: u64 = 8192;

/// A single-PV HP-UX volume group as seen from this disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HpuxLvm {
    pub pv_id: [u32; 2],
    pub vg_id: [u32; 2],
    /// Byte offset of physical extent 0.
    pub pe_start: u64,
    pub pe_size: u64,
    /// Physical extents on this disk.
    pub pe_count: u32,
    /// `(LV number, volume)` for every LV slot in use.
    pub volumes: Vec<(u32, LogicalVolume)>,
    /// Physical volumes the VGDA lists; more than one means a multi-disk group.
    pub pv_count: u32,
    /// The disk starts with an HP LIF boot volume.
    pub has_lif: bool,
    /// Byte offset of the first VGDA copy: the LIF boot volume must end before it.
    #[serde(default)]
    pub lif_end: u64,
}

impl HpuxLvm {
    /// Parse the LVM record and the newest VGDA copy, or `None` when the disk has none.
    pub fn detect<R: Read + Seek>(reader: &mut R) -> Option<Self> {
        let mut head = [0u8; 256];
        reader.seek(SeekFrom::Start(0)).ok()?;
        reader.read_exact(&mut head).ok()?;
        let has_lif = crate::fs::lif::looks_like_lif(&head);

        let mut rec = [0u8; 128];
        reader.seek(SeekFrom::Start(LVMREC_OFFSET)).ok()?;
        reader.read_exact(&mut rec).ok()?;
        if &rec[0..8] != LVMREC_MAGIC {
            return None;
        }
        let w = |o: usize| u32::from_be_bytes([rec[o], rec[o + 1], rec[o + 2], rec[o + 3]]);
        let pv_id = [w(8), w(12)];
        let vg_id = [w(16), w(20)];
        let vgda_len = w(40) as u64 * KIB;
        let pe_start = w(72) as u64 * KIB;
        let pe_size = w(80) as u64 * KIB;
        let shift = w(76);
        if pe_size == 0 || shift >= 64 || pe_size != 1u64 << shift || vgda_len == 0 {
            return None;
        }
        if vgda_len > MAX_VGDA_KIB * KIB {
            return None;
        }

        let vgda = [w(48), w(52)]
            .iter()
            .filter(|&&psn| psn != 0)
            .filter_map(|&psn| read_vgda(reader, psn as u64 * KIB, vgda_len, vg_id))
            .max_by_key(|v| (v.0, v.1))?
            .2;
        let mut lvm = Self::from_vgda(&vgda, pv_id, vg_id, pe_start, pe_size, has_lif)?;
        lvm.lif_end = [w(48), w(52)]
            .iter()
            .filter(|&&psn| psn != 0)
            .map(|&psn| psn as u64 * KIB)
            .min()
            .unwrap_or(pe_start)
            .min(pe_start);
        Some(lvm)
    }

    fn from_vgda(
        vgda: &[u8],
        pv_id: [u32; 2],
        vg_id: [u32; 2],
        pe_start: u64,
        pe_size: u64,
        has_lif: bool,
    ) -> Option<Self> {
        let h16 = |o: usize| u16::from_be_bytes([vgda[o], vgda[o + 1]]);
        let max_lvs = h16(16) as usize;
        let max_pe = h16(20) as usize;
        let lv_end = KIB as usize + max_lvs * 16;
        let pv_base = lv_end.div_ceil(KIB as usize) * KIB as usize;
        let pv_stride = (16 + max_pe * 4).div_ceil(KIB as usize) * KIB as usize;
        if max_lvs == 0 || max_pe == 0 || vgda.len() < pv_base + 16 {
            return None;
        }

        let mut pv_count = 0u32;
        let mut ours: Option<usize> = None;
        let mut at = pv_base;
        while at + 16 <= vgda.len() {
            let id = [
                u32::from_be_bytes([vgda[at], vgda[at + 1], vgda[at + 2], vgda[at + 3]]),
                u32::from_be_bytes([vgda[at + 4], vgda[at + 5], vgda[at + 6], vgda[at + 7]]),
            ];
            if id == [0, 0] {
                break;
            }
            pv_count += 1;
            if id == pv_id {
                ours = Some(at);
            }
            at += pv_stride;
        }
        let pv = ours?;
        let pe_count = h16(pv + 8) as u32;
        let map_end = pv + 16 + pe_count as usize * 4;
        if pe_count as usize > max_pe || map_end > vgda.len() {
            return None;
        }

        let mut found: Vec<Vec<(u64, u64)>> = vec![Vec::new(); max_lvs + 1];
        for pe in 0..pe_count as usize {
            let e = pv + 16 + pe * 4;
            let (lv, le) = (h16(e) as usize, h16(e + 2) as u64);
            if (1..=max_lvs).contains(&lv) {
                found[lv].push((le, pe_start + pe as u64 * pe_size));
            }
        }
        let volumes = (1..=max_lvs)
            .filter_map(|lv| {
                let extents = h16(KIB as usize + (lv - 1) * 16) as u64;
                (extents != 0).then(|| {
                    let vol = LogicalVolume::from_extents(
                        format!("lvol{lv}"),
                        extents,
                        pe_size,
                        std::mem::take(&mut found[lv]),
                    );
                    (lv as u32, vol)
                })
            })
            .collect();
        Some(Self {
            pv_id,
            vg_id,
            pe_start,
            pe_size,
            pe_count,
            volumes,
            pv_count,
            has_lif,
            lif_end: pe_start,
        })
    }
}

/// One VGDA copy whose VG id matches, as `(seconds, microseconds, bytes)`.
fn read_vgda<R: Read + Seek>(
    reader: &mut R,
    at: u64,
    len: u64,
    vg_id: [u32; 2],
) -> Option<(u32, u32, Vec<u8>)> {
    let mut buf = vec![0u8; len as usize];
    reader.seek(SeekFrom::Start(at)).ok()?;
    reader.read_exact(&mut buf).ok()?;
    let w = |o: usize| u32::from_be_bytes([buf[o], buf[o + 1], buf[o + 2], buf[o + 3]]);
    let (id, sec, usec) = ([w(8), w(12)], w(0), w(4));
    (id == vg_id).then_some((sec, usec, buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A disk shaped like the HP-UX 11.11 one, scaled down: 64 KiB extents, two LVs, one PV.
    fn sample_disk() -> Vec<u8> {
        let mut d = vec![0u8; 4 << 20];
        let put = |d: &mut Vec<u8>, o: usize, v: u32| d[o..o + 4].copy_from_slice(&v.to_be_bytes());
        d[8192..8200].copy_from_slice(LVMREC_MAGIC);
        put(&mut d, 8192 + 8, 0xAAAA_0001);
        put(&mut d, 8192 + 12, 0x0000_0002);
        put(&mut d, 8192 + 16, 0xBBBB_0001);
        put(&mut d, 8192 + 20, 0x0000_0003);
        put(&mut d, 8192 + 40, 20); // VGDA length, KiB
        put(&mut d, 8192 + 48, 64); // VGDA copy 1 at 64 KiB
        put(&mut d, 8192 + 52, 96); // VGDA copy 2 at 96 KiB (older)
        put(&mut d, 8192 + 72, 128); // first PE at 128 KiB
        put(&mut d, 8192 + 76, 16); // 64 KiB extents
        put(&mut d, 8192 + 80, 64);
        for (copy, stamp) in [(64usize, 10u32), (96, 5)] {
            let v = copy * 1024;
            put(&mut d, v, stamp);
            put(&mut d, v + 8, 0xBBBB_0001);
            put(&mut d, v + 12, 0x0000_0003);
            d[v + 16..v + 18].copy_from_slice(&4u16.to_be_bytes()); // max LVs
            d[v + 20..v + 22].copy_from_slice(&8u16.to_be_bytes()); // max PE per PV
                                                                    // lvol1 = 2 extents, lvol3 = 1 extent; the newer copy maps lvol1 to PEs 0, 1.
            d[v + 1024..v + 1026].copy_from_slice(&2u16.to_be_bytes());
            d[v + 1024 + 32..v + 1024 + 34].copy_from_slice(&1u16.to_be_bytes());
            let pv = v + 2048;
            put(&mut d, pv, 0xAAAA_0001);
            put(&mut d, pv + 4, 0x0000_0002);
            d[pv + 8..pv + 10].copy_from_slice(&3u16.to_be_bytes());
            let map: [(u16, u16); 3] = if stamp == 10 {
                [(1, 0), (1, 1), (3, 0)]
            } else {
                [(3, 0), (1, 0), (1, 1)]
            };
            for (i, (lv, le)) in map.iter().enumerate() {
                d[pv + 16 + i * 4..pv + 18 + i * 4].copy_from_slice(&lv.to_be_bytes());
                d[pv + 18 + i * 4..pv + 20 + i * 4].copy_from_slice(&le.to_be_bytes());
            }
        }
        d
    }

    #[test]
    fn maps_lvs_from_the_newest_vgda() {
        let lvm = HpuxLvm::detect(&mut Cursor::new(sample_disk())).expect("lvm");
        assert_eq!(
            (lvm.pe_start, lvm.pe_size, lvm.pe_count),
            (128 * 1024, 64 * 1024, 3)
        );
        assert_eq!(lvm.pv_count, 1);
        let names: Vec<&str> = lvm.volumes.iter().map(|(_, v)| v.name.as_str()).collect();
        assert_eq!(names, vec!["lvol1", "lvol3"]);
        assert_eq!(
            lvm.volumes[0].1.contiguous_start(),
            Some(128 * 1024),
            "newest copy wins"
        );
        assert_eq!(lvm.volumes[1].1.contiguous_start(), Some(256 * 1024));
    }

    #[test]
    fn no_lvm_record_is_none() {
        assert!(HpuxLvm::detect(&mut Cursor::new(vec![0u8; 65536])).is_none());
    }
}
