//! Flux-level floppy images, decoded read-only to a flat sector image.
//!
//! A flux image records the timing of every magnetic transition rather than the
//! sectors the controller would have returned, so a disk with marginal sectors
//! can still be read: every revolution in the capture is searched and the copy
//! whose CRC holds wins. The layers are kept apart so new capture formats and
//! encodings slot in independently:
//!
//! - a capture format turns its files into [`FluxTrack`]s (transition intervals
//!   in nanoseconds per cylinder / head): [`kryoflux`];
//! - an encoding turns a track into sectors: [`mfm`] (IBM-style MFM);
//! - [`decode_disk`] lays the sectors out by the cylinder / head / sector their
//!   own ID fields carry and reports what it could not read, which is zero-filled.

pub mod kryoflux;
pub mod mfm;

use std::collections::BTreeMap;

/// One captured track: transition intervals in nanoseconds, all revolutions back to back.
#[derive(Debug, Clone)]
pub struct FluxTrack {
    pub cyl: u32,
    pub head: u32,
    pub intervals_ns: Vec<u32>,
}

/// A sector the encoding layer found: its header address, data and whether both CRCs held.
#[derive(Debug, Clone)]
pub struct Sector {
    pub cyl: u8,
    pub head: u8,
    pub record: u8,
    pub data: Vec<u8>,
    pub ok: bool,
}

/// Why a sector slot in the image is not trustworthy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectorFault {
    /// Found, but no copy passed its data CRC; the best copy is in the image.
    Crc,
    /// No copy found at all; the slot is zero-filled.
    Missing,
}

/// A decoded disk: the flat image and the sectors it could not read cleanly.
#[derive(Debug, Clone)]
pub struct DecodedDisk {
    pub image: Vec<u8>,
    pub cylinders: u32,
    pub heads: u32,
    pub sectors_per_track: u32,
    pub sector_size: usize,
    pub faults: Vec<(u32, u32, u32, SectorFault)>,
}

impl DecodedDisk {
    /// One line naming the unreadable sectors, for a warning.
    pub fn fault_summary(&self) -> Option<String> {
        if self.faults.is_empty() {
            return None;
        }
        let shown: Vec<String> = self
            .faults
            .iter()
            .take(8)
            .map(|(c, h, r, f)| {
                format!(
                    "{c}/{h}/{r}{}",
                    if *f == SectorFault::Crc { " (crc)" } else { "" }
                )
            })
            .collect();
        let more = self.faults.len().saturating_sub(shown.len());
        Some(format!(
            "{} of {} sectors unreadable (c/h/s {}{})",
            self.faults.len(),
            self.cylinders * self.heads * self.sectors_per_track,
            shown.join(", "),
            if more > 0 {
                format!(", +{more} more")
            } else {
                String::new()
            }
        ))
    }
}

/// Decode every track with IBM MFM and lay the sectors out by their own ID fields, so a
/// 40-track disk captured on an 80-track drive lands on the right cylinders.
pub fn decode_disk(tracks: &[FluxTrack]) -> Option<DecodedDisk> {
    let mut found: BTreeMap<(u32, u32, u32), Sector> = BTreeMap::new();
    for t in tracks {
        for s in mfm::decode_track(&t.intervals_ns) {
            let key = (s.cyl as u32, s.head as u32, s.record as u32);
            match found.get(&key) {
                Some(old) if old.ok || !s.ok => {}
                _ => {
                    found.insert(key, s);
                }
            }
        }
    }
    // Geometry from sectors that read cleanly, ignoring records too rare to be the format's.
    let good: Vec<&Sector> = found.values().filter(|s| s.ok).collect();
    let mut sizes: BTreeMap<usize, usize> = BTreeMap::new();
    let mut records: BTreeMap<u32, usize> = BTreeMap::new();
    for s in &good {
        *sizes.entry(s.data.len()).or_default() += 1;
        *records.entry(s.record as u32).or_default() += 1;
    }
    let sector_size = *sizes.iter().max_by_key(|(_, n)| **n)?.0;
    let common = records.values().copied().max()?;
    let spt = records
        .iter()
        .filter(|(_, &n)| n * 4 >= common)
        .map(|(&r, _)| r)
        .max()?;
    let heads = good.iter().map(|s| s.head as u32).max()? + 1;
    let cylinders = good.iter().map(|s| s.cyl as u32).max()? + 1;
    let mut image = vec![0u8; (cylinders * heads * spt) as usize * sector_size];
    let mut faults = Vec::new();
    for c in 0..cylinders {
        for h in 0..heads {
            for r in 1..=spt {
                let lba = ((c * heads + h) * spt + r - 1) as usize;
                match found.get(&(c, h, r)) {
                    Some(s) => {
                        let n = s.data.len().min(sector_size);
                        image[lba * sector_size..lba * sector_size + n]
                            .copy_from_slice(&s.data[..n]);
                        if !s.ok {
                            faults.push((c, h, r, SectorFault::Crc));
                        }
                    }
                    None => faults.push((c, h, r, SectorFault::Missing)),
                }
            }
        }
    }
    Some(DecodedDisk {
        image,
        cylinders,
        heads,
        sectors_per_track: spt,
        sector_size,
        faults,
    })
}
