//! Resize Alto Disk: re-lay an Alto BFS volume onto a Diablo 31 or 44 pack and save it as a PDI.
//!
//! The source is read whole (Alto packs are a few MB) and never written; the output is always a
//! new file. `controller::alto_resize` drives it for the Inspect tab's dialog.

use std::path::Path;

use fs_xerox::alto::bfs::Bfs;
use fs_xerox::alto::{open_pack, pdi, write, FsFamily, Geometry};

/// The geometries an Alto volume can be re-laid onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AltoTarget {
    Diablo31,
    Diablo44,
}

impl AltoTarget {
    pub const ALL: [AltoTarget; 2] = [AltoTarget::Diablo31, AltoTarget::Diablo44];

    pub fn pages(self) -> usize {
        match self {
            AltoTarget::Diablo31 => 203 * 2 * 12,
            AltoTarget::Diablo44 => 406 * 2 * 12,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            AltoTarget::Diablo31 => "Diablo 31 - 203x2x12 (4872 pages, ~2.5 MB)",
            AltoTarget::Diablo44 => "Diablo 44 - 406x2x12 (9744 pages, ~5 MB)",
        }
    }

    pub fn geometry(self) -> Geometry {
        let (model, cylinders) = match self {
            AltoTarget::Diablo31 => (31, 203),
            AltoTarget::Diablo44 => (44, 406),
        };
        Geometry {
            family: FsFamily::Diablo,
            disk_model: model,
            n_disks: 1,
            n_cylinders: cylinders,
            n_heads: 2,
            n_sectors: 12,
            label_bytes: 16,
            data_bytes: 512,
        }
    }
}

/// What the dialog shows about the source pack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AltoSummary {
    /// "Current: Diablo N - DxCxHxS = P pages (U used)".
    pub line: String,
    pub used_pages: usize,
    /// The geometry the source is not, so Resize does something.
    pub suggested: AltoTarget,
}

/// Read the pack's geometry and used-page count.
pub fn summarize(path: &Path) -> Result<AltoSummary, String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    let disk = open_pack(bytes.as_slice()).map_err(|e| format!("{e}"))?;
    let g = &disk.geometry;
    let total = g.total_sectors();
    let free = Bfs::new(&disk)
        .disk_descriptor()
        .map(|d| d.free_pages as usize)
        .unwrap_or(0);
    let used = total.saturating_sub(free);
    Ok(AltoSummary {
        line: format!(
            "Current: Diablo {} - {}x{}x{}x{} = {} pages ({} used)",
            g.disk_model, g.n_disks, g.n_cylinders, g.n_heads, g.n_sectors, total, used
        ),
        used_pages: used,
        suggested: if g.disk_model == 31 {
            AltoTarget::Diablo44
        } else {
            AltoTarget::Diablo31
        },
    })
}

/// Clone the pack at `source` onto `target` and write it to `out` as a PDI; "Saved ... (N pages).".
pub fn resize_to(source: &Path, target: AltoTarget, out: &Path) -> Result<String, String> {
    let bytes = std::fs::read(source).map_err(|e| e.to_string())?;
    let src = open_pack(bytes.as_slice()).map_err(|e| format!("{e}"))?;
    let resized = write::clone_to(&src, target.geometry()).map_err(|e| format!("{e}"))?;
    std::fs::write(out, pdi::write(&resized)).map_err(|e| e.to_string())?;
    Ok(format!(
        "Saved {} ({} pages).",
        out.display(),
        resized.geometry.total_sectors()
    ))
}
