//! Solaris device enumeration and raw-device access.
//!
//! Solaris exposes disks twice: `/dev/dsk/...` is buffered, `/dev/rdsk/...` is the raw
//! character device. Only the raw node gives unbuffered access, and it demands that every
//! read and write be a whole multiple of the sector size -- an unaligned one fails EINVAL
//! rather than returning a short count, so `SectorAlignedWriter` is not optional here.
//!
//! **Enumeration deliberately opens nothing.** A wedged driver blocks in `open(2)` and
//! ignores `O_NONBLOCK`: measured on a Sun Blade 2500 running Solaris 9, where a USB stick
//! behind `scsa2usb` left processes unkillable in the kernel. Solaris' own tools have the
//! same problem -- `format(1M)` and `rmformat(1)` both hang on that disk -- so listing by
//! opening is not merely slow, it is a hang waiting for one bad device. Sizes therefore come
//! from the kstats the driver publishes, which is what `iostat -E` reads and the only source
//! that keeps working. See docs/solaris-raw-devices.md.
//!
//! This module uses `libc` and `libkstat` alone: `rb-cli-sol9` drops `nix`, which `linux.rs`
//! and `macos.rs` both rely on, so anything reached from here is a raw call.

#![cfg_attr(not(target_os = "solaris"), allow(dead_code))]

use crate::device::{DiskDevice, MountedPartition};
use std::ffi::CString;
use std::fs;
use std::os::raw::{c_char, c_int, c_void};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

/// From <sys/dkio.h>: DKIOC is (0x04 << 8), and DKIOCREMOVABLE is DKIOC|16.
const DKIOCREMOVABLE: c_int = (0x04 << 8) | 16;

/// <sys/kstat.h>: 30 chars plus NUL.
const KSTAT_STRLEN: usize = 31;
const KSTAT_DATA_CHAR: u8 = 0;
const KSTAT_DATA_UINT64: u8 = 4;

/// <sys/kstat.h> kstat_named_t. The value is a union whose largest arm is `char c[16]`, and
/// it follows a 31-byte name plus a type byte -- so the union lands 8-aligned at offset 32
/// and the whole struct is 48 bytes. Only this layout is needed: `kstat_t` stays opaque,
/// because `kstat_data_lookup` hands back a pointer into a buffer the library owns.
#[repr(C)]
struct KstatNamed {
    name: [c_char; KSTAT_STRLEN],
    data_type: u8,
    value: [u8; 16],
}

#[cfg_attr(target_os = "solaris", link(name = "kstat"))]
extern "C" {
    fn kstat_open() -> *mut c_void;
    fn kstat_close(kc: *mut c_void) -> c_int;
    fn kstat_lookup(
        kc: *mut c_void,
        module: *const c_char,
        inst: c_int,
        name: *const c_char,
    ) -> *mut c_void;
    fn kstat_read(kc: *mut c_void, ksp: *mut c_void, buf: *mut c_void) -> c_int;
    fn kstat_data_lookup(ksp: *mut c_void, name: *const c_char) -> *mut c_void;
}

/// An open kstat chain. Solaris publishes one `<driver>err` set per disk instance, carrying
/// the size and the inquiry strings without anyone having to touch the device.
struct Kstat(*mut c_void);

impl Kstat {
    fn open() -> Option<Self> {
        // SAFETY: no arguments, and a null return is the documented failure.
        let kc = unsafe { kstat_open() };
        if kc.is_null() {
            return None;
        }
        Some(Kstat(kc))
    }

    /// Read one named statistic out of `<driver>err:<instance>`.
    fn named(&self, driver: &str, instance: i32, field: &str) -> Option<Named> {
        let module = CString::new(format!("{driver}err")).ok()?;
        let field = CString::new(field).ok()?;
        // SAFETY: the chain is open, and a null name matches any kstat in the module.
        unsafe {
            let ksp = kstat_lookup(
                self.0,
                module.as_ptr(),
                instance as c_int,
                std::ptr::null::<c_char>(),
            );
            if ksp.is_null() || kstat_read(self.0, ksp, std::ptr::null_mut()) == -1 {
                return None;
            }
            let p = kstat_data_lookup(ksp, field.as_ptr()) as *const KstatNamed;
            if p.is_null() {
                return None;
            }
            let n = &*p;
            match n.data_type {
                KSTAT_DATA_UINT64 => Some(Named::U64(u64::from_ne_bytes(
                    n.value[..8].try_into().ok()?,
                ))),
                // A char value is a fixed 16-byte field, NUL-padded rather than terminated.
                KSTAT_DATA_CHAR => {
                    let end = n
                        .value
                        .iter()
                        .position(|&b| b == 0)
                        .unwrap_or(n.value.len());
                    Some(Named::Str(
                        String::from_utf8_lossy(&n.value[..end]).trim().to_string(),
                    ))
                }
                _ => None,
            }
        }
    }

    fn size(&self, driver: &str, instance: i32) -> Option<u64> {
        match self.named(driver, instance, "Size")? {
            Named::U64(v) => Some(v),
            Named::Str(_) => None,
        }
    }

    fn text(&self, driver: &str, instance: i32, field: &str) -> String {
        match self.named(driver, instance, field) {
            Some(Named::Str(s)) => s,
            _ => String::new(),
        }
    }
}

impl Drop for Kstat {
    fn drop(&mut self) {
        // SAFETY: the pointer came from kstat_open and is closed exactly once.
        unsafe { kstat_close(self.0) };
    }
}

enum Named {
    U64(u64),
    Str(String),
}

/// The whole-disk node for a disk, which is spelled differently per architecture: SPARC has
/// only SMI slices and calls slice 2 the whole disk, while x86 wraps those in an fdisk table
/// and calls the whole disk `p0`. Both spellings are probed because the Oracle documentation
/// shows `p0` throughout and someone reading it would otherwise "fix" this to match.
fn whole_disk_node(disk: &str) -> Option<PathBuf> {
    for suffix in ["p0", "s2"] {
        let p = PathBuf::from("/dev/rdsk").join(format!("{disk}{suffix}"));
        if p.exists() {
            return Some(p);
        }
    }
    None
}

/// Split `c0t0d0s2` or `c1t0d0p0` into the disk name and its slice or partition suffix.
fn split_node(name: &str) -> Option<(&str, &str)> {
    let idx = name.rfind(['s', 'p'])?;
    let (disk, suffix) = name.split_at(idx);
    if disk.is_empty() || suffix.len() < 2 || !suffix[1..].bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((disk, suffix))
}

/// Map each `/dev/rdsk` disk to its driver and instance, which is what a kstat is keyed on.
/// The node is a symlink into /devices; `/etc/path_to_inst` names the instance for that
/// physical path. Both are reads -- no device is opened.
fn instances() -> Vec<(String, String, i32)> {
    let table = fs::read_to_string("/etc/path_to_inst").unwrap_or_default();
    let mut by_path = Vec::new();
    for line in table.lines() {
        // "/pci@1d,700000/scsi@4/sd@0,0" 2 "sd"
        let mut parts = line.split('"');
        let path = match parts.nth(1) {
            Some(p) => p,
            None => continue,
        };
        let rest = parts.next().unwrap_or("").trim();
        let driver = parts.next().unwrap_or("");
        if let Ok(inst) = rest.parse::<i32>() {
            by_path.push((path.to_string(), driver.to_string(), inst));
        }
    }

    let entries = match fs::read_dir("/dev/rdsk") {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<(String, String, i32)> = Vec::new();
    for entry in entries.flatten() {
        let name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(_) => continue,
        };
        let disk = match split_node(&name) {
            Some((d, sfx)) if sfx == "s2" || sfx == "p0" => d.to_string(),
            _ => continue,
        };
        if out.iter().any(|(d, _, _)| *d == disk) {
            continue;
        }
        // The link points at /devices/<path>:<minor>; the instance table keys on <path>.
        let link = match fs::read_link(entry.path()) {
            Ok(l) => l,
            Err(_) => continue,
        };
        let link = link.to_string_lossy();
        let phys = match link.split("/devices").nth(1) {
            Some(p) => p.split(':').next().unwrap_or("").to_string(),
            None => continue,
        };
        if let Some((_, driver, inst)) = by_path.iter().find(|(p, _, _)| *p == phys) {
            out.push((disk, driver.clone(), *inst));
        }
    }
    out.sort();
    out
}

/// Mount points keyed by their device name, read from the live mount table.
fn mounts() -> Vec<(String, PathBuf, String)> {
    let table = fs::read_to_string("/etc/mnttab").unwrap_or_default();
    let mut out = Vec::new();
    for line in table.lines() {
        let mut f = line.split_whitespace();
        let (special, mount_point, fstype) = match (f.next(), f.next(), f.next()) {
            (Some(a), Some(b), Some(c)) => (a, b, c),
            _ => continue,
        };
        if !special.starts_with("/dev/dsk/") {
            continue;
        }
        out.push((
            special.trim_start_matches("/dev/dsk/").to_string(),
            PathBuf::from(mount_point),
            fstype.to_string(),
        ));
    }
    out
}

/// Whether the media is removable. Only called for disks whose kstat reports a real size, so
/// the device is known to be answering before anything opens it.
fn is_removable(path: &Path) -> bool {
    let file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
    {
        Ok(f) => f,
        Err(_) => return false,
    };
    let mut removable: c_int = 0;
    // SAFETY: the fd is open for the call and the driver writes a single int.
    let rc = unsafe { libc::ioctl(file.as_raw_fd(), DKIOCREMOVABLE as _, &mut removable) };
    rc == 0 && removable != 0
}

/// Enumerate disks from the kstats their drivers publish, never opening a device to do it.
pub fn enumerate_devices() -> Vec<DiskDevice> {
    let kstat = match Kstat::open() {
        Some(k) => k,
        None => return Vec::new(),
    };
    let mounted = mounts();

    let mut devices = Vec::new();
    for (disk, driver, instance) in instances() {
        // A zero size means the device is not answering -- an empty drive, or one whose
        // driver has wedged. Opening it to find out is exactly what must not happen.
        let size_bytes = match kstat.size(&driver, instance) {
            Some(s) if s > 0 => s,
            _ => continue,
        };
        let path = match whole_disk_node(&disk) {
            Some(p) => p,
            None => continue,
        };

        let vendor = kstat.text(&driver, instance, "Vendor");
        let product = kstat.text(&driver, instance, "Product");
        let media_name = format!("{vendor} {product}").trim().to_string();

        let partitions: Vec<MountedPartition> = mounted
            .iter()
            .filter(|(special, _, _)| split_node(special).map(|(d, _)| d) == Some(&disk[..]))
            .map(|(special, mount_point, fstype)| MountedPartition {
                name: special.clone(),
                mount_point: mount_point.clone(),
                filesystem: fstype.clone(),
                total_space: 0,
                available_space: 0,
            })
            .collect();

        // A disk carrying the root filesystem is the system disk; writing it is refused.
        let is_system = partitions.iter().any(|p| p.mount_point == Path::new("/"));
        let removable = is_removable(&path);

        devices.push(DiskDevice {
            name: disk,
            path,
            size_bytes,
            is_removable: removable,
            is_read_only: false,
            is_system,
            bus_protocol: driver,
            media_name,
            partitions,
        });
    }
    devices
}

#[cfg(test)]
mod tests {
    use super::split_node;

    #[test]
    fn a_sparc_slice_node_splits_into_disk_and_slice() {
        assert_eq!(split_node("c0t0d0s2"), Some(("c0t0d0", "s2")));
        assert_eq!(split_node("c1t3d0s0"), Some(("c1t3d0", "s0")));
    }

    /// x86 wraps the slices in an fdisk table and calls the whole disk p0.
    #[test]
    fn an_x86_fdisk_node_splits_the_same_way() {
        assert_eq!(split_node("c3t0d0p0"), Some(("c3t0d0", "p0")));
        assert_eq!(split_node("c3t0d0p1"), Some(("c3t0d0", "p1")));
    }

    #[test]
    fn a_name_that_is_not_a_node_is_rejected() {
        assert_eq!(split_node("c0t0d0"), None);
        assert_eq!(split_node(""), None);
        assert_eq!(split_node("c0t0d0sx"), None);
    }
}
