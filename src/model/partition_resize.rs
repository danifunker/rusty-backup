//! Resize Partitions: the size-field rule and the in-place write behind the Inspect tab's dialog.
//!
//! [`parsed_new_size`] is the one parser the preview, the layout bar and the apply share (they
//! once disagreed, and every cylinder-rounded SGI / EFS size was dropped from the apply).
//! [`run_resize`] opens the image or device read-write, applies `partition::resize::apply_resize`
//! and trims a shrunk image file. `controller::resize` drives both. `rb-cli resize` is a different
//! operation (one filesystem inside its partition) and does not use this.

use std::io::Seek;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::model::status::ResizeStatus;
use crate::partition::resize::{apply_resize, detect_vhd, PartitionResizePlan};
use crate::partition::PartitionTable;

const MIB: f64 = 1024.0 * 1024.0;

/// A size as the field shows it: MiB with two decimals.
pub fn stamp_mib(bytes: u64) -> String {
    format!("{:.2}", bytes as f64 / MIB)
}

/// The size typed into a field, in sector-aligned bytes; `None` when it is not a number.
///
/// The original's own stamp reads back as exactly the original size: the two-decimal round trip
/// would otherwise move a size that is not a whole MiB by up to ~5 KiB.
pub fn parsed_new_size(text: &str, original_size: u64) -> Option<u64> {
    let text = text.trim();
    if text == stamp_mib(original_size) {
        return Some(original_size);
    }
    let mib: f64 = text.parse().ok()?;
    if !mib.is_finite() || mib < 0.0 {
        return None;
    }
    let bytes = (mib * MIB) as u64;
    Some((bytes / 512) * 512)
}

/// Bytes the resize moves, for its progress total.
pub fn bytes_to_move(plans: &[PartitionResizePlan]) -> u64 {
    plans
        .iter()
        .filter(|p| p.needs_data_move)
        .map(|p| p.old_size_bytes)
        .sum()
}

/// Apply `plans` to the image or device at `path` in place; progress and log lines go to `status`.
pub fn run_resize(
    path: &Path,
    is_device: bool,
    plans: &[PartitionResizePlan],
    table: &PartitionTable,
    disk_size: u64,
    status: &Arc<Mutex<ResizeStatus>>,
) -> anyhow::Result<()> {
    let mut file = if is_device {
        crate::os::open_target_for_writing(path)?.file
    } else {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?
    };

    let file_size = file.seek(std::io::SeekFrom::End(0))?;
    file.seek(std::io::SeekFrom::Start(0))?;
    let is_vhd = detect_vhd(&mut file, file_size);

    apply_resize(
        &mut file,
        plans,
        table,
        is_device,
        is_vhd,
        disk_size,
        &mut |current, total| {
            let mut s = crate::model::worker::lock_status(&**status);
            s.current_bytes = current;
            s.total_bytes = total;
        },
        &mut |msg| {
            crate::model::worker::lock_status(&**status)
                .log_messages
                .push(msg.to_string());
        },
    )?;

    // apply_resize works on any Read + Write + Seek; only a real file can be truncated here.
    if !is_device {
        let new_data_end = plans
            .iter()
            .map(|p| p.new_start_lba * 512 + p.new_size_bytes)
            .max()
            .unwrap_or(disk_size);
        let old_data_size = if is_vhd {
            disk_size.saturating_sub(512)
        } else {
            disk_size
        };
        if new_data_end < old_data_size {
            let target = if is_vhd {
                new_data_end + 512
            } else {
                new_data_end
            };
            file.set_len(target)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The apply once parsed the two-decimal field as `u64`, so every partition whose size is
    /// not a whole MiB (all SGI / EFS ones) dropped out and the resize "succeeded" doing nothing.
    #[test]
    fn a_fractional_mib_size_is_parsed_not_dropped() {
        let original = 207_470_592;
        let got = parsed_new_size("91.34", original).expect("a two-decimal size must parse");
        assert_eq!(got, (((91.34 * MIB) as u64) / 512) * 512);
        assert_ne!(got, original, "must register as a change");
    }

    #[test]
    fn the_original_stamp_round_trips_to_the_exact_original_size() {
        let original = 207_470_592u64;
        let stamped = stamp_mib(original);
        assert_eq!(parsed_new_size(stamped.as_str(), original), Some(original));
    }

    #[test]
    fn sizes_are_sector_aligned_and_junk_is_rejected() {
        assert_eq!(parsed_new_size("1.0009765625", 999), Some(1_049_600));
        assert_eq!(parsed_new_size("", 999), None);
        assert_eq!(parsed_new_size("big", 999), None);
        assert_eq!(parsed_new_size("-5", 999), None);
    }
}
