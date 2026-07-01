// `browse_view` is an egui widget; it only exists in GUI builds. The CLI/mini
// build enables `optical` without `gui`, so gate it to keep that build green.
#[cfg(feature = "gui")]
pub mod browse_view;
// CD-DA playback for the Optical tab's in-app audio player. `audio` implies
// `chd` + `optical`, so the libchdman_rs / cd-da-reader imports always resolve.
#[cfg(feature = "audio")]
pub mod cd_audio;
pub mod convert;
pub mod rip;
pub mod source;

pub use convert::ConvertProgress;
pub use rip::{run_rip, OpticalTarget, RipConfig, RipFormat, RipProgress};
#[cfg(feature = "remote")]
pub use source::RemoteCdReader;
pub use source::{LocalCdReader, OpticalSource};

/// Map opticaldiscs' raw per-file timestamps to the Mac-1904 creation /
/// modification seconds that MacBinary/AppleDouble carry. HFS (local) and HFS+
/// (GMT) dates are already stored as seconds since 1904-01-01 — the exact
/// MacBinary encoding — so they drop straight in with no conversion.
/// Filesystems without Mac dates (ISO 9660, EFS) return the "unknown" default,
/// which extraction writes as zero dates just as before.
pub fn mac_dates_from(
    ts: &Option<opticaldiscs::FileTimestamps>,
) -> crate::fs::resource_fork::MacFileDates {
    use opticaldiscs::FileTimestamps as T;
    let (created, modified) = match ts {
        Some(T::Hfs {
            created, modified, ..
        }) => (*created, *modified),
        Some(T::HfsPlus {
            created,
            content_modified,
            ..
        }) => (*created, *content_modified),
        _ => (0, 0),
    };
    crate::fs::resource_fork::MacFileDates { created, modified }
}

#[cfg(test)]
mod tests {
    use super::mac_dates_from;
    use opticaldiscs::FileTimestamps;

    #[test]
    fn hfs_maps_created_and_modified() {
        let ts = Some(FileTimestamps::Hfs {
            created: 111,
            modified: 222,
            backup: 333,
        });
        let d = mac_dates_from(&ts);
        assert_eq!((d.created, d.modified), (111, 222));
    }

    #[test]
    fn hfsplus_maps_created_and_content_modified() {
        let ts = Some(FileTimestamps::HfsPlus {
            created: 10,
            content_modified: 20,
            attribute_modified: 30,
            accessed: 40,
            backup: 50,
        });
        let d = mac_dates_from(&ts);
        assert_eq!((d.created, d.modified), (10, 20));
    }

    #[test]
    fn non_mac_timestamps_are_unknown() {
        // EFS/Unix, ISO 9660, and absent timestamps carry no Mac dates, so the
        // extractor falls back to zero ("unknown") dates.
        let unix = mac_dates_from(&Some(FileTimestamps::Unix {
            atime: 1,
            mtime: 2,
            ctime: 3,
        }));
        assert_eq!((unix.created, unix.modified), (0, 0));
        let none = mac_dates_from(&None);
        assert_eq!((none.created, none.modified), (0, 0));
    }
}
