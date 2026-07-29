//! Assembling a Darwin disk list from plain POSIX facts.
//!
//! The real [`super::macos`] gets its device list from IOKit + DiskArbitration
//! through `objc2-*`, which cannot transpile for PowerPC — so the `os-stub`
//! build had no device list at all, and every device-driven flow (the TUI's
//! disk picker, `inspect`, `backup`) had nothing to show. Everything the
//! engine's [`DiskDevice`] actually needs is available without those
//! frameworks:
//!
//! | field | source |
//! |---|---|
//! | `name` / `path` | `readdir("/dev")` |
//! | `size_bytes` | `ioctl(DKIOCGETBLOCKSIZE / DKIOCGETBLOCKCOUNT)` |
//! | `is_read_only` | `ioctl(DKIOCISWRITABLE)` |
//! | `partitions`, `is_system` | `getmntinfo(3)` |
//!
//! `is_removable`, `bus_protocol` and `media_name` are the exception: they
//! exist only in IOKit's property tree, so this reports the honest defaults
//! rather than guessing.
//!
//! The syscalls live in the platform module; the assembly lives here, with no
//! `libc` and no `cfg`, so it compiles and is tested on the development
//! machine. That matters more than usual for this target: a mistake in the
//! PowerPC-only path is not caught by `cargo build` and costs an 80-minute
//! round trip to discover.

use std::path::{Path, PathBuf};

use crate::device::{DiskDevice, MountedPartition};

/// One mounted filesystem, as reported by `getmntinfo(3)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountEntry {
    /// `f_mntfromname`, e.g. `/dev/disk0s5`.
    pub from: String,
    /// `f_mntonname`, e.g. `/`.
    pub on: String,
    /// `f_fstypename`, e.g. `hfs`.
    pub fs_type: String,
    pub total_bytes: u64,
    pub available_bytes: u64,
}

/// Split a BSD disk name into its disk number and optional slice number:
/// `"disk0"` → `(0, None)`, `"disk0s5"` → `(0, Some(5))`.
///
/// Returns `None` for anything else in `/dev`, which is most of it.
pub fn parse_bsd_name(name: &str) -> Option<(u32, Option<u32>)> {
    let rest = name.strip_prefix("disk")?;
    let (disk_digits, rest) = split_digits(rest);
    if disk_digits.is_empty() {
        return None;
    }
    let disk: u32 = disk_digits.parse().ok()?;
    if rest.is_empty() {
        return Some((disk, None));
    }
    let slice_digits = rest.strip_prefix('s')?;
    let (slice_digits, tail) = split_digits(slice_digits);
    // A nested slice (`disk0s2s1`, seen on APM-inside-APM) is not a partition
    // of `disk0` and must not be attached to it.
    if slice_digits.is_empty() || !tail.is_empty() {
        return None;
    }
    Some((disk, Some(slice_digits.parse().ok()?)))
}

fn split_digits(s: &str) -> (&str, &str) {
    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    s.split_at(end)
}

/// What asking the device itself yields, for the fields no mount table knows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiskProbe {
    /// Byte size, or 0 when the device could not be opened.
    pub size_bytes: u64,
    /// From `DKIOCISWRITABLE`; `false` when unknown, which is the safer
    /// default for a flag the UI uses to decide whether a write can proceed.
    pub is_read_only: bool,
}

/// Build the device list from names found in `/dev`, the mount table, and a
/// way to interrogate a whole disk.
///
/// `probe` is passed a BSD name (`"disk0"`) and answers what only the device
/// can say. An unopenable disk comes back as `DiskProbe::default()` — the
/// normal unprivileged case — and deliberately stays in the list: the user
/// needs to see it in order to be told to re-run with privileges.
pub fn assemble<F>(dev_names: &[String], mounts: &[MountEntry], mut probe: F) -> Vec<DiskDevice>
where
    F: FnMut(&str) -> DiskProbe,
{
    let mut wholes: Vec<(u32, String)> = dev_names
        .iter()
        .filter_map(|n| match parse_bsd_name(n) {
            Some((disk, None)) => Some((disk, n.clone())),
            _ => None,
        })
        .collect();
    wholes.sort_by_key(|(disk, _)| *disk);
    wholes.dedup_by_key(|(disk, _)| *disk);

    wholes
        .into_iter()
        .map(|(disk_no, name)| {
            let partitions: Vec<MountedPartition> = mounts
                .iter()
                .filter(|m| mount_belongs_to(m, disk_no))
                .map(|m| MountedPartition {
                    name: m
                        .from
                        .rsplit('/')
                        .next()
                        .unwrap_or(m.from.as_str())
                        .to_string(),
                    mount_point: PathBuf::from(&m.on),
                    filesystem: m.fs_type.clone(),
                    total_space: m.total_bytes,
                    available_space: m.available_bytes,
                })
                .collect();

            // The disk carrying `/` is the one a restore must never target by
            // accident, and the one the UI marks as the system disk.
            let is_system = partitions.iter().any(|p| p.mount_point == Path::new("/"));

            let probed = probe(&name);
            DiskDevice {
                size_bytes: probed.size_bytes,
                path: PathBuf::from(format!("/dev/{name}")),
                name,
                // IOKit-only. Reporting `false` / empty is the honest answer
                // for a build without those frameworks; guessing "removable"
                // from the device number would be worse than saying nothing,
                // because the UI gates destructive actions on it. (`is_read_only`
                // is not in this group - the device answers that one directly.)
                is_removable: false,
                is_read_only: probed.is_read_only,
                is_system,
                bus_protocol: String::new(),
                media_name: String::new(),
                partitions,
            }
        })
        .collect()
}

/// The mounts that have to come down before `target_bsd` can be written.
///
/// `target_bsd` is a BSD name with no `/dev/` and no leading `r`
/// (`"disk2"`, `"disk2s1"`). Naming a whole disk selects every volume on it;
/// naming one slice selects only that slice, so a caller restoring into a
/// single partition does not tear down its neighbours.
///
/// Returns references in the order given, and never matches a pseudo-filesystem
/// (`devfs`, `map -hosts`) since those have no `/dev/` device.
pub fn mounts_to_unmount<'a>(target_bsd: &str, mounts: &'a [MountEntry]) -> Vec<&'a MountEntry> {
    let Some((disk_no, target_slice)) = parse_bsd_name(target_bsd) else {
        return Vec::new();
    };
    mounts
        .iter()
        .filter(|m| {
            let Some(bsd) = m.from.strip_prefix("/dev/") else {
                return false;
            };
            match parse_bsd_name(bsd) {
                Some((d, Some(s))) if d == disk_no => {
                    // Whole disk named: every slice. One slice named: only it.
                    target_slice.is_none() || target_slice == Some(s)
                }
                _ => false,
            }
        })
        .collect()
}

/// Whether a mount lives on the given whole disk. Parsed rather than matched
/// by prefix: `"disk1"` is a prefix of `"disk10s1"`, so a string comparison
/// hangs partitions off the wrong disk once a machine has ten of them.
fn mount_belongs_to(m: &MountEntry, disk_no: u32) -> bool {
    let Some(bsd) = m.from.strip_prefix("/dev/") else {
        return false; // `devfs`, `map -hosts`, ... are not on any disk
    };
    matches!(parse_bsd_name(bsd), Some((d, Some(_))) if d == disk_no)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mount(from: &str, on: &str) -> MountEntry {
        MountEntry {
            from: from.to_string(),
            on: on.to_string(),
            fs_type: "hfs".to_string(),
            total_bytes: 1000,
            available_bytes: 400,
        }
    }

    #[test]
    fn parses_whole_disks_and_slices() {
        assert_eq!(parse_bsd_name("disk0"), Some((0, None)));
        assert_eq!(parse_bsd_name("disk12"), Some((12, None)));
        assert_eq!(parse_bsd_name("disk0s5"), Some((0, Some(5))));
        assert_eq!(parse_bsd_name("disk10s1"), Some((10, Some(1))));
    }

    #[test]
    fn rejects_everything_else_in_dev() {
        for name in [
            "disk",
            "diskx",
            "rdisk0",
            "console",
            "null",
            "tty",
            "disk0s",
            "disk0x1",
            // A nested slice belongs to the slice, not to the disk.
            "disk0s2s1",
        ] {
            assert_eq!(parse_bsd_name(name), None, "{name} should not parse");
        }
    }

    /// `"disk1"` is a string prefix of `"disk10s1"`. Matching by prefix would
    /// hand disk10's partitions to disk1 on any machine with ten disks.
    #[test]
    fn partitions_attach_by_number_not_by_prefix() {
        let names = vec!["disk1".to_string(), "disk10".to_string()];
        let mounts = vec![
            mount("/dev/disk1s1", "/Volumes/One"),
            mount("/dev/disk10s1", "/Volumes/Ten"),
        ];
        let devices = assemble(&names, &mounts, |_| DiskProbe {
            size_bytes: 512,
            is_read_only: false,
        });

        let one = devices.iter().find(|d| d.name == "disk1").unwrap();
        let ten = devices.iter().find(|d| d.name == "disk10").unwrap();
        assert_eq!(one.partitions.len(), 1, "{:?}", one.partitions);
        assert_eq!(one.partitions[0].name, "disk1s1");
        assert_eq!(ten.partitions.len(), 1, "{:?}", ten.partitions);
        assert_eq!(ten.partitions[0].name, "disk10s1");
    }

    /// Shaped after the real `getmntinfo` output from the G5, pseudo-filesystems
    /// included — those have no `/dev/` device and must not land on a disk.
    #[test]
    fn assembles_the_g5_layout() {
        let names = vec![
            "disk1".to_string(),
            "disk0".to_string(), // out of order on purpose
        ];
        let mounts = vec![
            mount("/dev/disk0s5", "/"),
            mount("devfs", "/dev"),
            mount("map -hosts", "/net"),
            mount("/dev/disk0s2", "/Volumes/untitled"),
            mount("/dev/disk0s3", "/Volumes/MacOS TigerLNX"),
            mount("/dev/disk1s2", "/Volumes/PowerPC DevTools 2025"),
        ];
        let devices = assemble(&names, &mounts, |n| DiskProbe {
            size_bytes: if n == "disk0" { 256_060_514_304 } else { 0 },
            is_read_only: false,
        });

        assert_eq!(devices.len(), 2);
        // Sorted by disk number regardless of readdir order.
        assert_eq!(devices[0].name, "disk0");
        assert_eq!(devices[1].name, "disk1");

        let disk0 = &devices[0];
        assert_eq!(disk0.path, PathBuf::from("/dev/disk0"));
        assert_eq!(disk0.size_bytes, 256_060_514_304);
        assert!(disk0.is_system, "the disk holding / is the system disk");
        assert_eq!(disk0.partitions.len(), 3, "{:?}", disk0.partitions);

        let disk1 = &devices[1];
        assert!(!disk1.is_system);
        assert_eq!(disk1.partitions.len(), 1);
        // Unopenable (unprivileged) must still be listed, so the UI can show it
        // and explain why it has no size.
        assert_eq!(disk1.size_bytes, 0);
    }

    /// Restoring into one partition must not unmount its neighbours, and
    /// naming the whole disk must take all of them down. The disk1/disk10 trap
    /// applies here too - unmounting the wrong volume is worse than listing it
    /// under the wrong heading.
    #[test]
    fn unmount_targets_follow_the_named_scope() {
        let mounts = vec![
            mount("/dev/disk2s1", "/Volumes/One"),
            mount("/dev/disk2s2", "/Volumes/Two"),
            mount("/dev/disk20s1", "/Volumes/Twenty"),
            mount("devfs", "/dev"),
            mount("/dev/disk0s5", "/"),
        ];

        let whole: Vec<&str> = mounts_to_unmount("disk2", &mounts)
            .iter()
            .map(|m| m.on.as_str())
            .collect();
        assert_eq!(
            whole,
            vec!["/Volumes/One", "/Volumes/Two"],
            "whole disk takes all its slices"
        );

        let one: Vec<&str> = mounts_to_unmount("disk2s2", &mounts)
            .iter()
            .map(|m| m.on.as_str())
            .collect();
        assert_eq!(one, vec!["/Volumes/Two"], "a named slice takes only itself");

        // Nothing for an unrelated disk, and pseudo-filesystems never match.
        assert!(mounts_to_unmount("disk3", &mounts).is_empty());
        assert!(mounts_to_unmount("not-a-disk", &mounts).is_empty());
    }

    #[test]
    fn iokit_only_properties_are_not_invented() {
        let names = vec!["disk0".to_string()];
        let devices = assemble(&names, &[], |_| DiskProbe {
            size_bytes: 1024,
            is_read_only: false,
        });
        assert!(!devices[0].is_removable);
        assert!(devices[0].bus_protocol.is_empty());
        assert!(devices[0].media_name.is_empty());
    }
}
