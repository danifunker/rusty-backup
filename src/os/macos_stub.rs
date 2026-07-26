//! Stand-in for [`super::macos`] on a Mac we have no platform layer for.
//!
//! Selected in place of `os/macos.rs` by the `os-stub` feature (see
//! `os/mod.rs`). It exists for `powerpc-apple-darwin`: that target reports
//! `target_os = "macos"`, so the real module would be compiled, and the real
//! module is built on IOKit and DiskArbitration through `objc2-*` plus ~50
//! `libc` calls. `objc2` binds modern macOS frameworks and will not transpile
//! for a 2005 PowerPC Mac, so the whole file has to go - and with it the
//! `objc2-*` crates leave the dependency graph.
//!
//! What is lost is **raw device access**: enumerating physical disks, claiming
//! them from DiskArbitration, and `authopen`. What is kept is everything that
//! operates on a *file*, which is the entire first-cut scope for PowerPC - the
//! engine reads and writes disk *images*. Raw-device support arrives later with
//! the hand-written C platform shell (see docs/native_osx_10_dot_3.md).
//!
//! Every function here keeps the signature of its counterpart in `macos.rs`, so
//! `os/mod.rs` and its callers are untouched. Where an operation genuinely
//! cannot be done, the error names the host OS via [`HostVersion`] - on these
//! machines "which Mac OS X is this" is the first thing worth knowing, and it is
//! what tells 10.4 from 10.5.

use anyhow::{bail, Result};
use std::fs::File;
use std::path::Path;

use super::host_version::HostVersion;
use super::ElevatedSource;
use crate::device::DiskDevice;

/// Explain a missing platform facility, naming the OS we are actually on.
fn unsupported(what: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "{what} needs the native macOS device layer, which this build does not \
         include (host: {}). Disk *image* files work normally; raw device access \
         requires a build with the platform layer compiled in.",
        HostVersion::detect()
    )
}

/// No-op. The real version clears `F_NOCACHE` so an HFS+ inspect fd goes through
/// the buffer cache, which is a throughput optimisation for raw devices only -
/// and raw devices are exactly what this build does not open. Regular files are
/// already cached, so there is nothing to clear and nothing to report.
pub fn clear_nocache(_file: &File) -> std::io::Result<()> {
    Ok(())
}

/// Size of an already-open file.
///
/// The real version issues `DKIOCGETBLOCKCOUNT`/`DKIOCGETBLOCKSIZE` because a
/// raw character device reports length 0 through `stat`. Here every handle is a
/// regular file, so its metadata length is the honest answer.
pub fn get_device_size(file: &std::fs::File) -> Option<u64> {
    file.metadata().ok().map(|m| m.len())
}

/// No physical devices are visible without the native device layer.
///
/// Returning empty rather than erroring matches the existing
/// `#[cfg(not(any(macos, linux, windows)))]` arm in `os/mod.rs`: callers treat
/// this as "no removable media found" and carry on, which is the truthful answer
/// here.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    Vec::new()
}

/// Placeholder for the DiskArbitration claim the real module holds for the life
/// of an operation. Nothing to hold, so nothing to release; kept as a type so
/// the struct fields and signatures in `os/mod.rs` are unchanged.
#[derive(Debug)]
pub(crate) struct DiskClaim;

pub(crate) fn open_device_for_inspect(path: &Path) -> Result<File> {
    Ok(File::open(path)?)
}

pub(crate) fn open_target_for_writing(path: &Path) -> Result<(File, Option<DiskClaim>)> {
    if is_device_path(path) {
        return Err(unsupported("writing to a raw device"));
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    Ok((file, None))
}

pub fn open_source_for_reading(path: &Path) -> Result<ElevatedSource> {
    if is_device_path(path) {
        return Err(unsupported("reading from a raw device"));
    }
    // `ElevatedSource`'s fields are private to `os`, and this module is a child
    // of it - the same way the real `macos.rs` constructs one.
    Ok(ElevatedSource {
        file: File::open(path)?,
        temp_path: None,
        disk_claim: None,
    })
}

/// No optical drive to claim without DiskArbitration.
pub(crate) fn claim_optical_disc(_device_path: &str) -> Option<DiskClaim> {
    None
}

pub fn authopen_optical_device(_device_path: &str) -> Result<File> {
    bail!(unsupported("opening an optical device via authopen"))
}

/// Whether a path names a device node rather than a file, so the functions above
/// can fail with a useful message instead of a bare `ENOENT`/`EINVAL` from the
/// kernel. Matching on the path is enough: `/dev/disk*` and `/dev/rdisk*` are
/// the only shapes the callers construct.
fn is_device_path(path: &Path) -> bool {
    path.to_str()
        .map(|s| s.starts_with("/dev/disk") || s.starts_with("/dev/rdisk"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_paths_are_recognised() {
        assert!(is_device_path(Path::new("/dev/disk2")));
        assert!(is_device_path(Path::new("/dev/rdisk2s1")));
        assert!(!is_device_path(Path::new("/Users/me/image.img")));
        assert!(!is_device_path(Path::new("relative.dmg")));
    }

    #[test]
    fn no_devices_are_enumerated() {
        assert!(enumerate_devices().is_empty());
    }

    /// The point of routing errors through `HostVersion`: the message has to say
    /// which OS it is, or it is no better than "unsupported platform".
    #[test]
    fn unsupported_error_names_the_host() {
        let msg = unsupported("reading from a raw device").to_string();
        assert!(msg.contains("raw device"), "{msg}");
        assert!(msg.contains("host: "), "{msg}");
        assert!(msg.contains(HostVersion::arch()), "{msg}");
    }

    #[test]
    fn device_paths_are_refused_but_files_are_not_prejudged() {
        assert!(open_source_for_reading(Path::new("/dev/rdisk9")).is_err());
        // A regular path must fail (if at all) on its own I/O error, not on a
        // blanket "unsupported" - that distinction is what keeps images working.
        let err = open_source_for_reading(Path::new("/nonexistent/image.img"))
            .unwrap_err()
            .to_string();
        assert!(
            !err.contains("needs the native macOS device layer"),
            "{err}"
        );
    }

    #[test]
    fn size_of_a_regular_file_is_its_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sized.img");
        std::fs::write(&path, [0u8; 1024]).unwrap();
        let f = File::open(&path).unwrap();
        assert_eq!(get_device_size(&f), Some(1024));
    }
}
