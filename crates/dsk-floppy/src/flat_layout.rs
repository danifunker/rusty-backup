//! Lay decoded sector-dump tracks (EDSK, D88) out as a flat CHS image.
//!
//! Each physical track keeps its slot even when the dump omits it or some of
//! its sectors: a missing sector is zero-filled so every later sector stays at
//! the offset a filesystem driver computes from the geometry. The geometry of a
//! track is its own sector size and ID run, widened to the disk's dominant run
//! (the most common first ID and sector count among tracks of the same size),
//! so a track that lost its first or last sector is still padded.

use std::collections::HashMap;

/// One sector as the container recorded it.
pub(crate) struct RawSector {
    pub id: u8,
    /// Logical size from the size code; the data is cut or padded to it.
    pub size: usize,
    pub data: Vec<u8>,
}

/// What [`flatten`] had to patch over.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct LayoutGaps {
    /// Sectors zero-filled because the dump does not hold them.
    pub missing: usize,
    /// Sectors dropped: a duplicate ID, or an ID outside the track's run.
    pub dropped: usize,
}

fn mode<T: Copy + Eq + std::hash::Hash + Ord>(items: impl Iterator<Item = T>) -> Option<T> {
    let mut counts: HashMap<T, usize> = HashMap::new();
    for i in items {
        *counts.entry(i).or_default() += 1;
    }
    // Ties go to the smaller value so the result does not depend on hash order.
    counts
        .into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
        .map(|(v, _)| v)
}

/// Flatten `tracks` (indexed by physical position; `None` = not in the dump).
/// Trailing absent tracks add nothing.
pub(crate) fn flatten(tracks: &[Option<Vec<RawSector>>]) -> (Vec<u8>, LayoutGaps) {
    // Per present track: (sector size, first ID, last ID).
    let shape = |secs: &Vec<RawSector>| -> Option<(usize, u8, u8)> {
        let size = mode(secs.iter().map(|s| s.size))?;
        let ids = secs.iter().filter(|s| s.size == size).map(|s| s.id);
        let first = ids.clone().min()?;
        let last = ids.max()?;
        Some((size, first, last))
    };
    let shapes: Vec<Option<(usize, u8, u8)>> =
        tracks.iter().map(|t| t.as_ref().and_then(shape)).collect();
    let dom_size = mode(shapes.iter().flatten().map(|s| s.0));
    let dom_first = mode(
        shapes
            .iter()
            .flatten()
            .filter(|s| Some(s.0) == dom_size)
            .map(|s| s.1),
    );
    let dom_count = mode(
        shapes
            .iter()
            .flatten()
            .filter(|s| Some(s.0) == dom_size && Some(s.1) == dom_first)
            .map(|s| s.2 as usize - s.1 as usize + 1),
    );
    let last_present = tracks.iter().rposition(|t| t.is_some());

    let mut out = Vec::new();
    let mut gaps = LayoutGaps::default();
    let Some(last_present) = last_present else {
        return (out, gaps);
    };
    for (ti, track) in tracks.iter().enumerate().take(last_present + 1) {
        let (Some(secs), Some(&Some((size, own_first, own_last)))) = (track, shapes.get(ti)) else {
            // An absent or empty track: hold its slot at the dominant geometry.
            if let (Some(size), Some(count)) = (dom_size, dom_count) {
                out.resize(out.len() + size * count, 0);
                gaps.missing += count;
            }
            if let Some(secs) = track {
                gaps.dropped += secs.len();
            }
            continue;
        };
        let (mut first, mut last) = (own_first as usize, own_last as usize);
        if Some(size) == dom_size {
            if let (Some(df), Some(dc)) = (dom_first, dom_count) {
                let df = df as usize;
                if first >= df && first < df + dc {
                    first = df;
                    last = last.max(df + dc - 1);
                }
            }
        }
        let mut used = vec![false; secs.len()];
        for id in first..=last {
            let hit = secs
                .iter()
                .enumerate()
                .find(|(i, s)| !used[*i] && s.id as usize == id && s.size == size);
            match hit {
                Some((i, s)) => {
                    used[i] = true;
                    let take = s.data.len().min(size);
                    out.extend_from_slice(&s.data[..take]);
                    out.resize(out.len() + (size - take), 0);
                }
                None => {
                    out.resize(out.len() + size, 0);
                    gaps.missing += 1;
                }
            }
        }
        gaps.dropped += used.iter().filter(|u| !**u).count();
    }
    (out, gaps)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(ids: &[u8], size: usize, fill: u8) -> Option<Vec<RawSector>> {
        Some(
            ids.iter()
                .map(|&id| RawSector {
                    id,
                    size,
                    data: vec![fill.wrapping_add(id); size],
                })
                .collect(),
        )
    }

    #[test]
    fn missing_sector_and_track_hold_their_slots() {
        let tracks = vec![
            track(&[1, 2, 3], 4, 0),
            None,
            track(&[1, 3], 4, 0x10), // sector 2 lost
            track(&[1, 2], 4, 0x20), // sector 3 lost
        ];
        let (flat, gaps) = flatten(&tracks);
        assert_eq!(flat.len(), 4 * 3 * 4);
        assert_eq!(
            gaps,
            LayoutGaps {
                missing: 3 + 1 + 1,
                dropped: 0
            }
        );
        assert_eq!(&flat[2 * 12 + 8..2 * 12 + 12], &[0x13; 4]);
        assert_eq!(&flat[2 * 12 + 4..2 * 12 + 8], &[0; 4]);
        assert_eq!(&flat[3 * 12 + 8..3 * 12 + 12], &[0; 4]);
    }

    #[test]
    fn odd_track_keeps_its_own_geometry() {
        // A smaller-sector track 0 (as on PC-98 2HD) is laid out at its own size.
        let tracks = vec![
            track(&[1, 2], 2, 0),
            track(&[1, 2, 3], 4, 0),
            track(&[1, 2, 3], 4, 0),
        ];
        let (flat, gaps) = flatten(&tracks);
        assert_eq!(flat.len(), 2 * 2 + 2 * 12);
        assert_eq!(gaps, LayoutGaps::default());
    }
}
