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
//! What is lost is what only those frameworks can answer. Enumerating disks,
//! sizing them and reading them turn out not to need frameworks at all -
//! `readdir("/dev")`, the `DKIOC*` ioctls and `getmntinfo(3)` cover it, and
//! [`super::darwin_devices`] assembles the result (with the tests, which run on
//! any host). What genuinely goes missing is the IOKit property tree -
//! removable / bus protocol / marketing name - and the one DiskArbitration
//! operation that has no POSIX equivalent: *claiming* a disk,
//! which keeps other DA clients and the Finder from remounting it mid-write.
//! Unmounting does have one - `unmount(2)`, present on both 10.4 and 10.5 - so
//! restores work here; they simply run without the exclusive claim the native
//! build holds. That claim arrives with the hand-written C platform shell (see
//! docs/native_osx_10_dot_3.md).
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

/// Clear `F_NOCACHE` so reads go through the buffer cache.
///
/// This used to be a no-op, on the grounds that the stub never opened a raw
/// device - which stopped being true when device enumeration and reading
/// landed. It matters for exactly the case it was written for: an HFS+ inspect
/// walks a B-tree in small scattered reads, and with `F_NOCACHE` set every one
/// of them is a synchronous trip to the platter. It is a single `fcntl`, with
/// no framework behind it, so there is no reason for the stub to differ.
pub fn clear_nocache(file: &File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    const F_NOCACHE: libc::c_int = 48;
    if unsafe { libc::fcntl(file.as_raw_fd(), F_NOCACHE, 0) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

// `_IOR('d', 24, u32)` / `_IOR('d', 25, u64)`, same values the real module
// uses. Plain `ioctl` numbers - no framework behind them.
const DKIOCGETBLOCKSIZE: libc::c_ulong = 0x40046418;
const DKIOCGETBLOCKCOUNT: libc::c_ulong = 0x40086419;
// `_IOR('d', 29, uint32_t)`. Present in the 10.5 SDK's <sys/disk.h>, checked
// on the machine - unlike DKIOCISVIRTUAL and the removable/ejectable
// properties, which are 10.6+ or IOKit-only.
const DKIOCISWRITABLE: libc::c_ulong = 0x4004641D;

/// Byte size of an open *device* via the disk ioctls, or `None` for anything
/// that is not one (a regular file answers `ENOTTY`).
fn ioctl_device_size(file: &File) -> Option<u64> {
    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();
    let mut block_size: u32 = 0;
    let mut block_count: u64 = 0;
    unsafe {
        if libc::ioctl(fd, DKIOCGETBLOCKSIZE, &mut block_size) != 0 {
            return None;
        }
        if libc::ioctl(fd, DKIOCGETBLOCKCOUNT, &mut block_count) != 0 {
            return None;
        }
    }
    (block_size != 0).then(|| block_count * block_size as u64)
}

/// Size of an already-open file or device.
///
/// A raw device reports length 0 through `stat` *and* through `lseek(END)` -
/// both measured on 10.5 - so the ioctls are the only source. Regular files
/// fall through to their metadata length.
pub fn get_device_size(file: &std::fs::File) -> Option<u64> {
    ioctl_device_size(file).or_else(|| file.metadata().ok().map(|m| m.len()))
}

/// Whether the media is writable, via `DKIOCISWRITABLE`. `None` when the
/// device does not answer, which the caller treats as "assume writable" - the
/// UI must not mark a disk read-only on the strength of a failed ioctl.
fn ioctl_is_writable(file: &File) -> Option<bool> {
    use std::os::unix::io::AsRawFd;
    let mut writable: u32 = 0;
    let rc = unsafe { libc::ioctl(file.as_raw_fd(), DKIOCISWRITABLE, &mut writable) };
    (rc == 0).then_some(writable != 0)
}

/// Read a fixed-size C string field out of a `statfs`.
fn c_str_field(buf: &[libc::c_char]) -> String {
    // Safe: the kernel NUL-terminates these, and the slice bounds the read.
    unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

/// The mount table, via `getmntinfo(3)`.
///
/// The returned buffer belongs to libc and must not be freed. On this target
/// libc binds `getmntinfo$INODE64` (verified with `nm` against the linked
/// binary), so the `statfs` layout is the 64-bit-inode one that libc's Rust
/// declaration describes - field offsets cross-checked against the machine in
/// `probe/devlist.c`.
fn read_mounts() -> Vec<super::darwin_devices::MountEntry> {
    let mut buf: *mut libc::statfs = std::ptr::null_mut();
    let n = unsafe { libc::getmntinfo(&mut buf, libc::MNT_NOWAIT) };
    if n <= 0 || buf.is_null() {
        return Vec::new();
    }
    let entries = unsafe { std::slice::from_raw_parts(buf, n as usize) };
    entries
        .iter()
        .map(|s| super::darwin_devices::MountEntry {
            from: c_str_field(&s.f_mntfromname),
            on: c_str_field(&s.f_mntonname),
            fs_type: c_str_field(&s.f_fstypename),
            total_bytes: s.f_blocks.saturating_mul(s.f_bsize as u64),
            available_bytes: s.f_bavail.saturating_mul(s.f_bsize as u64),
        })
        .collect()
}

/// Every `diskN` / `diskNsM` node in `/dev`.
fn dev_disk_names() -> Vec<String> {
    let Ok(entries) = std::fs::read_dir("/dev") else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| super::darwin_devices::parse_bsd_name(n).is_some())
        .collect()
}

/// Ask a whole disk about itself, preferring the raw node. All-zero when it
/// cannot be opened, which unprivileged runs hit on the internal disk - the
/// disk still belongs in the list so the UI can show it and say why it has no
/// size.
fn whole_disk_probe(bsd: &str) -> super::darwin_devices::DiskProbe {
    for path in [format!("/dev/r{bsd}"), format!("/dev/{bsd}")] {
        let Ok(f) = File::open(&path) else { continue };
        let Some(size_bytes) = ioctl_device_size(&f) else {
            continue;
        };
        return super::darwin_devices::DiskProbe {
            size_bytes,
            is_read_only: !ioctl_is_writable(&f).unwrap_or(true),
        };
    }
    super::darwin_devices::DiskProbe::default()
}

/// Enumerate physical disks without IOKit or DiskArbitration.
///
/// `readdir("/dev")` for the disks, the disk ioctls for their sizes and
/// `getmntinfo(3)` for what is mounted where; [`super::darwin_devices`] does
/// the assembly and carries the tests. What this cannot know is the IOKit-only
/// metadata - removable, bus protocol, marketing name - which stays at its
/// defaults rather than being guessed, because the UI gates destructive actions
/// on `is_removable`. (`is_read_only` comes from the device itself via
/// `DKIOCISWRITABLE`.)
pub fn enumerate_devices() -> Vec<DiskDevice> {
    super::darwin_devices::assemble(&dev_disk_names(), &read_mounts(), whole_disk_probe)
}

/// Placeholder for the DiskArbitration claim the real module holds for the life
/// of an operation. Nothing to hold, so nothing to release; kept as a type so
/// the struct fields and signatures in `os/mod.rs` are unchanged.
#[derive(Debug)]
pub(crate) struct DiskClaim;

/// Explain a failed open. The real module escalates a permission failure
/// through `authopen`, which puts the system's authentication dialog on
/// screen; there is no such path here, so name the thing that does work
/// instead of returning a bare `EACCES`.
fn open_error(path: &Path, e: std::io::Error) -> anyhow::Error {
    if is_device_path(path) && e.kind() == std::io::ErrorKind::PermissionDenied {
        return anyhow::anyhow!(
            "cannot open {} - permission denied. Raw disks belong to root on \
             this system (host: {}), and this build has no privilege-escalation \
             dialog, so run the command under `sudo`.",
            path.display(),
            HostVersion::detect()
        );
    }
    anyhow::Error::new(e).context(format!("cannot open {}", path.display()))
}

pub(crate) fn open_device_for_inspect(path: &Path) -> Result<File> {
    File::open(path).map_err(|e| open_error(path, e))
}

/// `/dev/disk2` -> `/dev/rdisk2`. The raw node is unbuffered, which is what the
/// native module writes through and what makes a restore run at disk speed
/// rather than through the buffer cache.
fn raw_device_path(path: &Path) -> std::path::PathBuf {
    match path.to_str() {
        Some(s) if s.starts_with("/dev/disk") => {
            std::path::PathBuf::from(format!("/dev/r{}", &s["/dev/".len()..]))
        }
        _ => path.to_path_buf(),
    }
}

/// The BSD name behind a device path, with any `r` prefix removed:
/// `/dev/rdisk2s1` -> `disk2s1`.
fn bsd_name_of(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    let name = name.strip_prefix('r').unwrap_or(name);
    super::darwin_devices::parse_bsd_name(name)?;
    Some(name.to_string())
}

/// Take down the volumes that would otherwise be written underneath.
///
/// `unmount(2)` is plain libc and present on both 10.4 and 10.5, so this needs
/// no DiskArbitration. Failures are logged rather than fatal, matching the
/// native module: by the time a restore reaches here the CLI's
/// `device_safety` has already refused a target with mounted partitions
/// (unless the user overrode it), so a stubborn volume is a warning about a
/// decision already made rather than a new one.
fn unmount_target_volumes(path: &Path) {
    let Some(bsd) = bsd_name_of(path) else { return };
    let mounts = read_mounts();
    for m in super::darwin_devices::mounts_to_unmount(&bsd, &mounts) {
        let Ok(mount_point) = std::ffi::CString::new(m.on.as_str()) else {
            continue;
        };
        if unsafe { libc::unmount(mount_point.as_ptr(), libc::MNT_FORCE) } == 0 {
            log::info!("unmounted {} ({})", m.on, m.from);
        } else {
            log::warn!(
                "could not unmount {} ({}): {}",
                m.on,
                m.from,
                std::io::Error::last_os_error()
            );
        }
    }
}

pub(crate) fn open_target_for_writing(path: &Path) -> Result<(File, Option<DiskClaim>)> {
    if !is_device_path(path) {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        return Ok((file, None));
    }

    unmount_target_volumes(path);

    // No claim is returned, and that is the one thing this path cannot do: the
    // native module holds a DiskArbitration claim for the duration, which stops
    // other DA clients (and the Finder) remounting the disk mid-write. Here the
    // volumes are unmounted and nothing prevents the OS mounting them again if
    // it notices the disk. In practice the window is small and the restore holds
    // the raw device open, but it is a real difference from the native build.
    let raw = raw_device_path(path);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&raw)
        .map_err(|e| open_error(&raw, e))?;
    Ok((file, None))
}

pub fn open_source_for_reading(path: &Path) -> Result<ElevatedSource> {
    // `ElevatedSource`'s fields are private to `os`, and this module is a child
    // of it - the same way the real `macos.rs` constructs one.
    Ok(ElevatedSource {
        file: super::SourceHandle::File(File::open(path).map_err(|e| open_error(path, e))?),
        temp_path: None,
        disk_claim: None,
    })
}

/// Stand-in for the real module's shared authopen descriptor.
///
/// There is no `authopen` to escalate through here, so nothing is ever cached
/// and no value of this type is ever constructed. It exists so
/// [`super::SourceHandle`] keeps one shape across both modules.
pub enum SharedDevice {}

impl SharedDevice {
    pub fn try_clone(&self) -> std::io::Result<Self> {
        match *self {}
    }

    pub fn byte_len(&self) -> u64 {
        match *self {}
    }

    pub fn dup_as_file(&self) -> std::io::Result<File> {
        match *self {}
    }
}

impl std::io::Read for SharedDevice {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        match *self {}
    }
}

impl std::io::Write for SharedDevice {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        match *self {}
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match *self {}
    }
}

impl std::io::Seek for SharedDevice {
    fn seek(&mut self, _pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match *self {}
    }
}

/// Nothing is cached without `authopen`, so releasing is a no-op.
pub fn release_elevated_devices(_path: Option<&str>) {}

/// TEMP-DIAG counterpart. Reports the plain-`open(2)` outcome; there is no
/// escalation path here to describe.
pub fn probe_device_access(path: &str) -> Vec<String> {
    let mut out = vec![format!(
        "[perm] euid={} (no authopen on this build)",
        unsafe { libc::geteuid() },
    )];
    match std::fs::OpenOptions::new().read(true).open(path) {
        Ok(_) => out.push(format!("[perm] {path}: O_RDONLY ok")),
        Err(e) => out.push(format!("[perm] {path}: O_RDONLY failed - {e}")),
    }
    out
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
    fn enumeration_reports_real_disks_without_iokit() {
        // Runs on any Mac: every machine has at least disk0, and whatever comes
        // back must be shaped correctly even unprivileged (where sizes are 0
        // because the raw node cannot be opened).
        let devices = enumerate_devices();
        assert!(
            !devices.is_empty(),
            "a Mac always has at least one disk; got an empty list"
        );
        for d in &devices {
            assert!(d.name.starts_with("disk"), "{:?}", d.name);
            assert_eq!(d.path, std::path::PathBuf::from(format!("/dev/{}", d.name)));
            // IOKit-only properties stay at their defaults rather than guesses.
            assert!(!d.is_removable);
            assert!(d.bus_protocol.is_empty());
        }
        // Exactly one disk carries `/`.
        assert_eq!(
            devices.iter().filter(|d| d.is_system).count(),
            1,
            "exactly one disk should hold the root filesystem"
        );
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
    fn reads_and_writes_are_both_allowed_on_devices() {
        // Reading a device is now supported, so a *missing* device must fail on
        // its own I/O error rather than a blanket refusal.
        let err = open_source_for_reading(Path::new("/dev/rdisk99"))
            .unwrap_err()
            .to_string();
        assert!(!err.contains("needs the"), "{err}");

        // Writing a device is supported now, so a missing one must also fail
        // on its own I/O error rather than a blanket refusal.
        let err = open_target_for_writing(Path::new("/dev/rdisk99"))
            .unwrap_err()
            .to_string();
        assert!(!err.contains("needs the"), "{err}");

        // A regular path must fail (if at all) on its own I/O error.
        let err = open_source_for_reading(Path::new("/nonexistent/image.img"))
            .unwrap_err()
            .to_string();
        assert!(!err.contains("needs the"), "{err}");
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
