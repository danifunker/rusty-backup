//! Flux-level floppy images, decoded read-only to a flat sector image.
//!
//! A flux image records the timing of every magnetic transition rather than the
//! sectors the controller would have returned, so a disk with marginal sectors
//! can still be read: every revolution in the capture is searched and the copy
//! whose CRC holds wins. The layers are kept apart so new capture formats and
//! encodings slot in independently:
//!
//! - a capture format turns its files into [`FluxTrack`]s (transition intervals
//!   in nanoseconds per cylinder / head): [`kryoflux`], [`scp`] (Greaseweazle's
//!   native format), [`hfe`] (HxC bitstreams, cells turned into intervals);
//! - an encoding turns a track into sectors: [`mfm`] (IBM-style MFM), [`fm`]
//!   (IBM-style FM) and [`amiga`] (AmigaDOS MFM), picked by probing a few tracks;
//! - [`decode_disk`] lays the sectors out by the cylinder / head / sector their
//!   own ID fields carry and reports what it could not read, which is zero-filled.

pub mod amiga;
pub mod fm;
pub mod hfe;
pub mod kryoflux;
pub mod mfm;
pub mod mfm_emu;
pub mod scp;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Unreadable sectors of each flux set decoded this session, keyed by the path it was opened by.
static FAULTS: Mutex<BTreeMap<PathBuf, FaultReport>> = Mutex::new(BTreeMap::new());

/// What a decode could not read, kept for the GUI to show after the image opens.
#[derive(Debug, Clone)]
pub struct FaultReport {
    pub summary: String,
    /// One line per unreadable sector: `c/h/s, LBA n, missing|bad CRC`.
    pub sectors: Vec<String>,
}

/// Remember `disk`'s faults for `path` (or forget them when it read cleanly).
pub fn record_faults(path: &Path, disk: &DecodedDisk) {
    let Ok(mut map) = FAULTS.lock() else {
        return;
    };
    match disk.fault_summary() {
        None => {
            map.remove(path);
        }
        Some(summary) => {
            let sectors = disk
                .faults
                .iter()
                .map(|&(c, h, r, f)| {
                    let lba = (c * disk.heads + h) * disk.sectors_per_track + r - disk.first_record;
                    let why = match f {
                        SectorFault::Crc => "bad CRC, best copy kept",
                        SectorFault::Missing => "missing, zero-filled",
                    };
                    format!("{c}/{h}/{r}, LBA {lba}, {why}")
                })
                .collect();
            map.insert(path.to_path_buf(), FaultReport { summary, sectors });
        }
    }
}

/// Whether `path` is a single-file flux image (SCP or HFE), by extension and magic.
pub fn is_flux_file_path(path: &Path) -> bool {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    if !matches!(ext.as_deref(), Some("scp") | Some("hfe")) {
        return false;
    }
    let mut head = [0u8; 20];
    let n = std::fs::File::open(path)
        .and_then(|mut f| std::io::Read::read(&mut f, &mut head))
        .unwrap_or(0);
    scp::looks_like_scp(&head[..n]) || hfe::looks_like_hfe(&head[..n])
}

/// The tracks of a single-file flux image (SCP or HFE).
pub fn read_flux_file(path: &Path) -> std::io::Result<Vec<FluxTrack>> {
    let data = std::fs::read(path)?;
    let parsed = if scp::looks_like_scp(&data) {
        scp::parse(&data)
    } else {
        hfe::parse(&data)
    };
    parsed.map_err(crate::compat::io_other)
}

/// The faults recorded for `path`, if its decode found any.
pub fn recorded_faults(path: &Path) -> Option<FaultReport> {
    FAULTS.lock().ok()?.get(path).cloned()
}

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
    /// The data mark and stored CRC, so damaged copies can be voted and re-checked.
    pub mark: u8,
    pub crc: u16,
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
    /// The lowest record number on a track (1 for IBM PC, 0 for Acorn DFS).
    pub first_record: u32,
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

/// A track decoder: one encoding's sectors from a track's flux.
type TrackDecoder = fn(&[u32]) -> Vec<Sector>;

/// The encoding that reads the most clean sectors from a few sample tracks.
fn pick_encoding(tracks: &[FluxTrack]) -> TrackDecoder {
    let decoders: [TrackDecoder; 3] = [mfm::decode_track, amiga::decode_track, fm::decode_track];
    let step = (tracks.len() / 3).max(1);
    let sample: Vec<&FluxTrack> = tracks.iter().step_by(step).take(3).collect();
    let score = |d: &TrackDecoder| -> usize {
        sample
            .iter()
            .map(|t| d(&t.intervals_ns).iter().filter(|s| s.ok).count())
            .sum()
    };
    let mut best = decoders[0];
    let mut best_score = score(&best);
    for d in &decoders[1..] {
        // MFM is the common case; only switch when another encoding clearly reads more.
        let n = score(d);
        if n > best_score {
            best = *d;
            best_score = n;
        }
    }
    best
}

/// Decode every track and lay the sectors out by their own ID fields, so a 40-track
/// disk captured on an 80-track drive lands on the right cylinders.
pub fn decode_disk(tracks: &[FluxTrack]) -> Option<DecodedDisk> {
    let decode = pick_encoding(tracks);
    let mut found: BTreeMap<(u32, u32, u32), Sector> = BTreeMap::new();
    let mut damaged: BTreeMap<(u32, u32, u32), Vec<Sector>> = BTreeMap::new();
    for t in tracks {
        for s in decode(&t.intervals_ns) {
            let key = (s.cyl as u32, s.head as u32, s.record as u32);
            if !s.ok {
                damaged.entry(key).or_default().push(s.clone());
            }
            match found.get(&key) {
                Some(old) if old.ok || !s.ok => {}
                _ => {
                    found.insert(key, s);
                }
            }
        }
    }
    // No clean copy: vote the damaged ones byte by byte and keep the result only if its CRC holds.
    for (key, copies) in damaged {
        if found.get(&key).is_some_and(|s| !s.ok) {
            if let Some(voted) = mfm::vote(&copies) {
                found.insert(key, voted);
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
    let usual: Vec<u32> = records
        .iter()
        .filter(|(_, &n)| n * 4 >= common)
        .map(|(&r, _)| r)
        .collect();
    // Records usually count from 1; Acorn DFS and others start at 0.
    let first = *usual.iter().min()?;
    let spt = usual.iter().max()? - first + 1;
    let heads = good.iter().map(|s| s.head as u32).max()? + 1;
    let cylinders = good.iter().map(|s| s.cyl as u32).max()? + 1;
    let mut image = vec![0u8; (cylinders * heads * spt) as usize * sector_size];
    let mut faults = Vec::new();
    for c in 0..cylinders {
        for h in 0..heads {
            for r in first..first + spt {
                let lba = ((c * heads + h) * spt + r - first) as usize;
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
        first_record: first,
        faults,
    })
}
