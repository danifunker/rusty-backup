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

/// Open `path` as a disc image — use this rather than calling
/// [`opticaldiscs::detect::DiscImageInfo::open`] directly.
///
/// `DiscImageInfo::open` sniffs every container it knows, and its CHD
/// branch ends in MAME's `cdrom_file(chd_file*)` constructor, which
/// **throws** when the CHD carries no CD metadata — i.e. every ordinary
/// hard-disk CHD. A C++ exception cannot unwind through Rust frames, so the
/// process did not get an error back: it called `std::terminate` and
/// aborted. Opening a hard-disk CHD anywhere near an optical probe killed
/// the app outright.
///
/// **libchdman-rs 0.288.10 fixed that at the source** (its shims catch and
/// return a failure, and it gained a `NotCdMedia` error), so the abort is
/// no longer reachable through a current dependency tree — see the
/// `upstream_reports_an_error_...` test, which exercises the raw call. This
/// wrapper stays because it is still the better answer: the CHD's own info
/// record says "is this a CD?" without constructing a `cdrom_file` at all,
/// which is cheaper than a failed construction, gives a message naming the
/// actual problem, and keeps a dependency downgrade from being fatal.
/// Non-CHD paths pass straight through.
///
/// Returns the crate's own error type, so callers keep the distinction
/// between a hard I/O failure and "this isn't a disc image" — the refusal
/// arrives as `UnsupportedFormat`, which is what a hard-disk CHD is from
/// the optical layer's point of view.
pub fn open_disc_image(
    path: &std::path::Path,
) -> Result<opticaldiscs::detect::DiscImageInfo, opticaldiscs::OpticaldiscsError> {
    if crate::model::source_reader::is_chd_path(path) && !chd_is_cd_safe(path) {
        return Err(opticaldiscs::OpticaldiscsError::UnsupportedFormat(format!(
            "{} is a CHD with no CD metadata (a hard-disk CHD), not an optical disc image",
            path.display()
        )));
    }
    opticaldiscs::detect::DiscImageInfo::open(path)
}

/// Whether a CHD declares itself a CD, via the info record only.
///
/// Without the `chd` feature there is no way to ask, so the answer is "no"
/// — refusing to probe is the safe direction when the alternative is an
/// abort.
fn chd_is_cd_safe(path: &std::path::Path) -> bool {
    #[cfg(feature = "chd")]
    {
        crate::rbformats::chd::chd_is_cd(path).unwrap_or(false)
    }
    #[cfg(not(feature = "chd"))]
    {
        let _ = path;
        false
    }
}
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

/// Format a game disc's identity into a single ASCII line for logs and UI
/// labels, e.g. `Nintendo GameCube - Zelda [GALE01] (North America (NTSC-U))`.
/// Every part is best-effort; only the console name is always present.
/// No Unicode glyphs (see CLAUDE.md) — plain ASCII separators only.
pub fn format_game_identity(g: &opticaldiscs::GameDiscInfo) -> String {
    let mut s = g.console.display_name().to_string();
    if let Some(title) = &g.title {
        s.push_str(" - ");
        s.push_str(title);
    }
    if let Some(serial) = &g.serial {
        s.push_str(&format!(" [{serial}]"));
    }
    if let Some(region) = g.region {
        s.push_str(&format!(" ({})", region.display_name()));
    }
    s
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

    #[test]
    fn game_identity_formats_all_parts() {
        use opticaldiscs::gameid::GameDiscInfo;
        use opticaldiscs::{Console, Region};
        let g = GameDiscInfo {
            console: Console::GameCube,
            serial: Some("GALE01".into()),
            title: Some("Super Smash Bros. Melee".into()),
            region: Some(Region::NtscU),
            maker: None,
            version: None,
        };
        assert_eq!(
            super::format_game_identity(&g),
            "Nintendo GameCube - Super Smash Bros. Melee [GALE01] (North America (NTSC-U))"
        );
    }

    #[test]
    fn game_identity_console_only() {
        use opticaldiscs::gameid::GameDiscInfo;
        use opticaldiscs::Console;
        let g = GameDiscInfo {
            console: Console::SegaDreamcast,
            serial: None,
            title: None,
            region: None,
            maker: None,
            version: None,
        };
        assert_eq!(super::format_game_identity(&g), "Sega Dreamcast");
    }

    /// [`super::open_disc_image`]'s guard exists because a non-CD CHD used
    /// to abort the whole process inside MAME's `cdrom_file` constructor.
    /// libchdman-rs 0.288.10 fixed that at the source — its shims catch and
    /// return a failure — so this asserts the RAW upstream call is safe,
    /// deliberately bypassing the guard so a dependency downgrade is caught
    /// here rather than by a user's crash report.
    ///
    /// If it regresses, the test binary aborts instead of failing an
    /// assertion. That is the nature of a foreign exception; it is at least
    /// unmissable.
    #[test]
    #[cfg(feature = "chd")]
    fn upstream_reports_an_error_for_a_hard_disk_chd_instead_of_aborting() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data = vec![0x5Au8; 512 * 1024];
        let base = tmp.path().join("hd");
        crate::rbformats::chd::compress_chd(
            &mut std::io::Cursor::new(&data),
            &base,
            data.len() as u64,
            None,
            None,
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .expect("build a hard-disk CHD");
        let chd = base.with_extension("chd");

        // The call that used to throw across the FFI boundary.
        let chd_handle =
            libchdman_rs::Chd::open(chd.to_str().unwrap(), false, None).expect("open CHD");
        assert!(
            libchdman_rs::cd::list_tracks(&chd_handle).is_err(),
            "a hard-disk CHD has no tracks; upstream must say so, not abort"
        );
        drop(chd_handle);

        // And the container probe `is_container_path` used to reach through.
        assert!(opticaldiscs::detect::DiscImageInfo::open(&chd).is_err());
    }
}
