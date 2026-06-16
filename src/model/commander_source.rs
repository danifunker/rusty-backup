//! Source resolution for Commander Mode panes.
//!
//! Bridges a user-picked image file to the partition list its pane offers and
//! to the [`BrowseSession`] that actually opens a chosen partition. This is the
//! same two-step the Inspect tab performs inline (parse the table, then build a
//! session per partition); lifting it here keeps the GUI panes thin and lets
//! both panes share one code path.
//!
//! Container peeling (CHD / GHO / IMZ / flat floppy wrappers, and the
//! VHD / 2MG / DMG / DiskCopy image wrappers) is handled exactly the way
//! [`BrowseSession::open`] peels them, so the partition offsets this module
//! reports line up with the offsets the session later opens at.

use std::path::Path;

use anyhow::{Context, Result};

use crate::model::backup_loader::infer_fat_type_byte;
use crate::model::browse_session::BrowseSession;
use crate::model::source_reader;
use crate::partition::{PartitionInfo, PartitionTable};
use crate::rbformats::{self, BoxReadSeek, ImageFormat};

/// Open a reader over `path` with any container wrapper peeled off, matching
/// [`BrowseSession::open`]'s own peeling so partition offsets are consistent.
fn open_probe_reader(path: &Path) -> Result<BoxReadSeek> {
    // CHD / GHO / IMZ and the flat floppy wrappers decode to a flat sector
    // stream via the shared container reader.
    if source_reader::is_container_path(path) {
        return source_reader::open_read(path);
    }

    // Otherwise peel an image wrapper (VHD / 2MG / DMG / DiskCopy 4.2 / ...);
    // a Raw image falls through to a plain buffered file.
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    match rbformats::detect_image_format_with_path(file, Some(path)) {
        Ok(format) if !matches!(format, ImageFormat::Raw) => {
            let file2 =
                std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
            let (reader, _size) = rbformats::wrap_image_reader(file2, format)
                .with_context(|| format!("unwrap image {}", path.display()))?;
            Ok(reader)
        }
        _ => {
            let file =
                std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
            Ok(Box::new(std::io::BufReader::new(file)))
        }
    }
}

/// Parse the partition table of the image/container at `path` and return its
/// partition list. A partition-less (superfloppy) image yields a single
/// offset-0 entry whose `type_name` carries the detected filesystem hint.
pub fn probe_partitions(path: &Path) -> Result<Vec<PartitionInfo>> {
    let mut reader = open_probe_reader(path)?;
    let table = PartitionTable::detect(&mut reader)
        .with_context(|| format!("parsing partition table of {}", path.display()))?;
    Ok(table.partitions())
}

/// Build a [`BrowseSession`] for `part` within the image at `path`. Mirrors the
/// field assignment `BrowseView::open` performs: the absolute byte offset comes
/// from [`PartitionInfo::byte_offset`], and a zero type byte (superfloppies,
/// some GPT entries) is inferred from the partition's display name so FAT
/// volumes still dispatch to the right driver.
pub fn session_for(path: &Path, part: &PartitionInfo) -> BrowseSession {
    let partition_type = if part.partition_type_byte != 0 {
        part.partition_type_byte
    } else {
        infer_fat_type_byte(&part.type_name)
    };
    BrowseSession {
        source_path: Some(path.to_path_buf()),
        partition_offset: part.byte_offset(),
        partition_type,
        partition_type_string: part.partition_type_string.clone(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A blank FAT12 floppy is a superfloppy: probing yields exactly one
    /// offset-0 partition, and the session built for it opens the volume.
    #[test]
    fn probe_and_session_round_trip_on_superfloppy() {
        let flat = crate::fs::fat::create_blank_fat(737280, Some("PROBE")).unwrap();
        let tmp = tempfile::Builder::new().suffix(".img").tempfile().unwrap();
        std::fs::write(tmp.path(), &flat).unwrap();

        let parts = probe_partitions(tmp.path()).expect("probe");
        assert_eq!(parts.len(), 1, "superfloppy exposes a single partition");
        assert_eq!(parts[0].byte_offset(), 0);

        let session = session_for(tmp.path(), &parts[0]);
        assert_eq!(session.partition_offset, 0);
        // A FAT type byte was inferred from the superfloppy's hint name (the
        // exact FAT12/16/32 flavor is re-detected from the BPB at open time).
        assert!(matches!(session.partition_type, 0x01 | 0x06 | 0x0B | 0x0C));

        let mut fs = session.open().expect("open volume");
        assert_eq!(fs.volume_label(), Some("PROBE"));
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
    }
}
